import asyncio
from asyncio import Event
from secrets import token_hex
from time import time

from aiofiles.os import makedirs

# Try to import MEGA SDK with system-wide detection
MEGA_SDK_AVAILABLE = False
MEGA_IMPORT_ERROR = None


def _detect_and_import_mega():
    """Detect MEGA SDK installation system-wide and import"""
    import sys
    from pathlib import Path

    # System-wide search paths for MEGA SDK
    search_paths = [
        "/usr/local/lib/python3.13/dist-packages",
        "/usr/lib/python3/dist-packages",
        "/usr/lib/python3.13/dist-packages",
        "/usr/local/lib/python3.12/dist-packages",
        "/usr/lib/python3.12/dist-packages",
        f"{sys.prefix}/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages",
        f"{sys.prefix}/lib/python{sys.version_info.major}.{sys.version_info.minor}/dist-packages",
        "/usr/src/app/.venv/lib/python3.13/site-packages",
        "/opt/megasdk/lib/python3.13/site-packages",
    ]

    # Try standard import first
    try:
        from mega import MegaApi, MegaError, MegaListener, MegaRequest, MegaTransfer

        # Test basic functionality
        api = MegaApi("test")
        api.getVersion()
        return (
            True,
            None,
            (MegaApi, MegaError, MegaListener, MegaRequest, MegaTransfer),
        )
    except ImportError:
        pass
    except Exception as e:
        return False, f"MEGA SDK validation failed: {e}", None

    # Search system-wide paths
    for path in search_paths:
        mega_path = Path(path) / "mega"
        if mega_path.exists():
            try:
                if str(Path(path)) not in sys.path:
                    sys.path.insert(0, str(Path(path)))

                from mega import (
                    MegaApi,
                    MegaError,
                    MegaListener,
                    MegaRequest,
                    MegaTransfer,
                )

                # Test basic functionality
                api = MegaApi("test")
                api.getVersion()
                return (
                    True,
                    None,
                    (MegaApi, MegaError, MegaListener, MegaRequest, MegaTransfer),
                )
            except Exception:
                continue

    return False, "MEGA SDK not found in system-wide search", None


# Perform detection
try:
    success, error, classes = _detect_and_import_mega()
    if success:
        MegaApi, MegaError, MegaListener, MegaRequest, MegaTransfer = classes
        MEGA_SDK_AVAILABLE = True
        MEGA_IMPORT_ERROR = None
    else:
        raise ImportError(error)
except Exception as e:
    # Create dummy classes to prevent import errors
    class MegaApi:
        pass

    class MegaListener:
        pass

    class MegaRequest:
        TYPE_LOGIN = 1
        TYPE_FETCH_NODES = 2
        TYPE_GET_PUBLIC_NODE = 3
        TYPE_COPY = 4
        TYPE_EXPORT = 5
        TYPE_LOGOUT = 6

    class MegaTransfer:
        pass

    class MegaError:
        pass

    MEGA_SDK_AVAILABLE = False
    MEGA_IMPORT_ERROR = str(e)

from bot import LOGGER, task_dict, task_dict_lock
from bot.core.config_manager import Config
from bot.helper.ext_utils.bot_utils import async_to_sync, sync_to_async
from bot.helper.ext_utils.limit_checker import limit_checker
from bot.helper.ext_utils.links_utils import get_mega_link_type
from bot.helper.ext_utils.task_manager import (
    check_running_tasks,
    stop_duplicate_check,
)
from bot.helper.mirror_leech_utils.status_utils.mega_status import MegaDownloadStatus
from bot.helper.mirror_leech_utils.status_utils.queue_status import QueueStatus
from bot.helper.telegram_helper.message_utils import send_status_message


class MegaAppListener(MegaListener):
    _NO_EVENT_ON = (MegaRequest.TYPE_LOGIN, MegaRequest.TYPE_FETCH_NODES)
    NO_ERROR = "no error"

    def __init__(self, continue_event: Event, listener):
        self.continue_event = continue_event
        self.node = None
        self.public_node = None
        self.listener = listener
        self.is_cancelled = False
        self.error = None
        self.__bytes_transferred = 0
        self.__speed = 0
        self.__name = ""
        self.__start_time = time()
        super().__init__()

    @property
    def speed(self):
        return self.__speed

    @property
    def downloaded_bytes(self):
        return self.__bytes_transferred

    def onRequestFinish(self, api, request, error):
        # More robust error checking - ignore errors after cancellation or logout
        if self.is_cancelled:
            return

        error_str = str(error).lower() if error else "unknown error"
        if error_str != "no error":
            # Check if this is an authentication-related error during active operations
            if "access denied" in error_str.lower():
                # Only ignore access denied errors during cleanup/logout, not during active operations
                if hasattr(self, "_cleanup_mode") and self._cleanup_mode:
                    return

                # Check the request type to provide more specific error handling
                request_type = request.getType() if request else None

                # This is a real authentication error during active operations
                self.error = error.copy() if hasattr(error, "copy") else str(error)

                # Provide more specific error messages based on when the error occurred
                if request_type == MegaRequest.TYPE_LOGIN:
                    LOGGER.error(f"MEGA Login failed: Access denied - {self.error}")
                    friendly_msg = (
                        "❌ MEGA Login Failed: Access denied.\n\n"
                        "This usually means:\n"
                        "• Invalid MEGA email or password\n"
                        "• Account suspended or restricted\n"
                        "• Two-factor authentication enabled (not supported)\n\n"
                        "Please check your MEGA credentials in bot settings."
                    )
                elif request_type == MegaRequest.TYPE_FETCH_NODES:
                    LOGGER.error(
                        f"MEGA Node fetching failed: Access denied - {self.error}"
                    )
                    friendly_msg = (
                        "❌ MEGA Access Failed: Cannot access account data.\n\n"
                        "This usually means:\n"
                        "• Session expired during operation\n"
                        "• Account has restricted access\n"
                        "• Temporary MEGA server issue\n\n"
                        "Please try again in a few minutes."
                    )
                elif request_type == MegaRequest.TYPE_GET_PUBLIC_NODE:
                    LOGGER.error(
                        f"MEGA Public node access failed: Access denied - {self.error}"
                    )
                    friendly_msg = (
                        "❌ MEGA Link Access Failed: Cannot access the shared file/folder.\n\n"
                        "This usually means:\n"
                        "• The link is private or expired\n"
                        "• The file/folder has been removed\n"
                        "• Link requires account access\n\n"
                        "Please check the MEGA link and try again."
                    )
                else:
                    # Check if this is a cleanup-related error (after successful completion)
                    # Access denied errors during logout/cleanup are normal and should be ignored
                    request_type = request.getType() if request else None
                    if request_type == MegaRequest.TYPE_LOGOUT:
                        return  # Don't treat logout errors as real errors

                    LOGGER.error(
                        f"MEGA Authentication Error during active operation: {self.error}"
                    )
                    friendly_msg = (
                        "❌ MEGA Session Error: Access denied during download.\n\n"
                        "This usually means:\n"
                        "• Session expired during download\n"
                        "• Account download quota exceeded\n"
                        "• Concurrent download limit reached\n"
                        "• Temporary MEGA server issue\n\n"
                        "Please wait a few minutes and try again."
                    )

                async_to_sync(self.listener.on_download_error, friendly_msg)
                self.continue_event.set()
                return

            self.error = error.copy() if hasattr(error, "copy") else str(error)

            request_type = request.getType() if request else None
            self._get_request_type_name(request_type)
            error_code = (
                error.getErrorCode() if hasattr(error, "getErrorCode") else "unknown"
            )

            # Classify error severity based on error content
            if "error code: -1" in error_str:
                # Error code -1 is typically a temporary network/connection issue
                # Log as INFO since these are usually automatically retried and succeed
                LOGGER.warning(
                    f"MEGA temporary connection issue (will retry): {self.error}"
                )
            elif any(
                temp_error in error_str
                for temp_error in [
                    "error code: -2",
                    "error code: -3",
                    "error code: -4",
                    "error code: -5",
                ]
            ):
                # These are also typically temporary errors
                LOGGER.warning(f"MEGA temporary error (will retry): {self.error}")
            else:
                # Other error codes are more serious
                LOGGER.error(f"MEGA Request Error: {self.error}")

            # For login requests, provide additional context
            if request_type == MegaRequest.TYPE_LOGIN:
                LOGGER.error(
                    f"MEGA Login request failed - Error code: {error_code}, Message: {self.error}"
                )

            self.continue_event.set()
            return

        request_type = request.getType()
        self._get_request_type_name(request_type)

        if request_type == MegaRequest.TYPE_LOGIN:
            api.fetchNodes()
        elif request_type == MegaRequest.TYPE_GET_PUBLIC_NODE:
            self.public_node = request.getPublicMegaNode()
            self.__name = self.public_node.getName()
        elif request_type == MegaRequest.TYPE_FETCH_NODES:
            self.node = api.getRootNode()
            self.__name = self.node.getName()

        if request_type not in self._NO_EVENT_ON or (
            self.node and "cloud drive" not in self.__name.lower()
        ):
            self.continue_event.set()

    def _get_request_type_name(self, request_type):
        """Helper method to get human-readable request type name"""
        type_names = {
            MegaRequest.TYPE_LOGIN: "LOGIN",
            MegaRequest.TYPE_FETCH_NODES: "FETCH_NODES",
            MegaRequest.TYPE_GET_PUBLIC_NODE: "GET_PUBLIC_NODE",
            MegaRequest.TYPE_LOGOUT: "LOGOUT",
        }
        return type_names.get(request_type, f"UNKNOWN({request_type})")

    def onRequestTemporaryError(self, _api, _request, error: MegaError):
        error_msg = error.toString()
        error_code = (
            error.getErrorCode() if hasattr(error, "getErrorCode") else "unknown"
        )
        request_type = _request.getType() if _request else None
        request_type_name = self._get_request_type_name(request_type)

        LOGGER.warning(
            f"MEGA Request temporary error (will retry): {error_msg} [Type: {request_type_name}, Code: {error_code}]"
        )

        # Track retry count for this error type
        if not hasattr(self, "_retry_counts"):
            self._retry_counts = {}

        retry_key = f"{request_type}_{error_code}"
        self._retry_counts[retry_key] = self._retry_counts.get(retry_key, 0) + 1

        # Special handling for specific error codes
        if error_code == -3 and request_type == MegaRequest.TYPE_LOGIN:
            # Error code -3 (MEGA_ERROR_EARGS) during login - let MEGA SDK handle retries
            # Don't terminate early as MEGA SDK may eventually succeed
            if self._retry_counts[retry_key] >= 15:
                LOGGER.warning(
                    f"MEGA login error -3 has occurred {self._retry_counts[retry_key]} times - but continuing to let MEGA SDK retry"
                )
        elif error_code == -9 and request_type == MegaRequest.TYPE_LOGIN:
            # Error code -9 (MEGA_ERROR_EACCESS) during login means access denied
            # This is a permanent error, stop retrying
            LOGGER.error("MEGA login error -9 (access denied) - stopping retries")
            self.error = "Access denied (error -9)"
            self.continue_event.set()
            return

        # Temporary errors should NOT cancel the download - they will be retried automatically
        # Only log the error and let MEGA SDK handle the retry logic
        self.error = error_msg

        # Don't set continue_event or cancel - let MEGA SDK retry the request
        # The continue_event will be set when the request succeeds or fails permanently

    def onTransferUpdate(self, api: MegaApi, transfer: MegaTransfer):
        try:
            if self.is_cancelled:
                LOGGER.info("MEGA transfer cancelled, stopping transfer...")
                api.cancelTransfer(transfer, None)
                self.continue_event.set()
                return

            self.__speed = transfer.getSpeed()
            self.__bytes_transferred = transfer.getTransferredBytes()

            # Always log transfer updates to debug progress issues
            try:
                transfer.getFileName() if transfer else "Unknown"
                total_bytes = transfer.getTotalBytes() if transfer else 0
                transfer.getState() if transfer else 0

                # Log progress periodically
                if not hasattr(self, "_last_progress_log"):
                    self._last_progress_log = 0

                current_time = time()
                if (
                    current_time - self._last_progress_log >= 5
                ):  # Log every 5 seconds
                    if total_bytes > 0:
                        (self.__bytes_transferred / total_bytes * 100)
                        self.__speed / (1024 * 1024) if self.__speed > 0 else 0
                    self._last_progress_log = current_time
            except Exception as e:
                # Log but don't crash
                LOGGER.warning(f"MEGA progress logging error: {e}")
        except Exception as e:
            # Critical: Prevent any exceptions from propagating to SWIG
            LOGGER.error(f"MEGA onTransferUpdate error: {e}")
            # Don't re-raise the exception to prevent SWIG director method error

    def onTransferFinish(self, _api: MegaApi, transfer: MegaTransfer, error):
        try:
            transfer_name = transfer.getFileName() if transfer else "Unknown"
            transfer.getTotalBytes() if transfer else 0
            transfer.getTransferredBytes() if transfer else 0
            is_folder = transfer.isFolderTransfer() if transfer else False
            transfer.isFinished() if transfer else False

            if self.is_cancelled:
                LOGGER.info("MEGA transfer was cancelled")
                self.continue_event.set()
                return

            if str(error).lower() != "no error":
                error_msg = str(error)
                LOGGER.error(f"MEGA transfer finished with error: {error_msg}")

                # Check if this is an access denied error and provide specific handling
                if "access denied" in error_msg.lower():
                    LOGGER.warning(
                        f"MEGA transfer finished with access denied: {error_msg}"
                    )
                    # Provide user-friendly message for transfer access denied errors
                    friendly_msg = (
                        "❌ MEGA Download Failed: Access denied during transfer.\n\n"
                        "This usually means:\n"
                        "• Download quota exceeded for your account\n"
                        "• File is no longer accessible or was removed\n"
                        "• Temporary MEGA server restrictions\n"
                        "• Session expired during download\n\n"
                        "Please wait a few hours and try again, or check if the file is still available."
                    )
                    self.error = friendly_msg
                    async_to_sync(self.listener.on_download_error, friendly_msg)
                else:
                    LOGGER.error(f"MEGA transfer finished with error: {error_msg}")
                    self.error = error_msg
                    async_to_sync(
                        self.listener.on_download_error,
                        f"MEGA Transfer Error: {error_msg}",
                    )
            elif transfer.isFinished():
                # Check if this is the right transfer to complete
                # For folder transfers, we need to handle name mismatches between original name and SDK-generated name
                is_main_folder_transfer = False

                if is_folder:
                    # Check if this is the main folder transfer by comparing names
                    # The transfer name might be "MEGA_Folder" while original name is "test folder"
                    if transfer_name == self.__name:
                        is_main_folder_transfer = True
                    elif (
                        transfer_name == "MEGA_Folder"
                        and self.__name != "MEGA_Folder"
                    ):
                        # Handle case where MEGA SDK uses default "MEGA_Folder" name
                        is_main_folder_transfer = True
                    elif (
                        hasattr(self.listener, "name")
                        and transfer_name == self.listener.name
                    ):
                        # Check against listener name as well
                        is_main_folder_transfer = True

                if is_folder and is_main_folder_transfer:
                    # For folder transfers, only complete when the main folder transfer finishes
                    # Individual files finishing should not trigger completion
                    try:
                        async_to_sync(self.listener.on_download_complete)
                    except Exception as e:
                        LOGGER.error(f"Error calling on_download_complete: {e}")
                        import traceback

                        LOGGER.error(f"Full traceback: {traceback.format_exc()}")
                elif not is_folder and transfer_name == self.__name:
                    # For single file transfers
                    try:
                        async_to_sync(self.listener.on_download_complete)
                    except Exception as e:
                        LOGGER.error(f"Error calling on_download_complete: {e}")
                else:
                    # Individual files in a folder transfer - don't trigger completion yet
                    # DON'T set continue_event for individual files - only for main folder
                    return
            else:
                LOGGER.warning(
                    f"MEGA transfer finished but not marked as finished - {transfer_name}"
                )

            self.continue_event.set()
        except Exception as e:
            # Critical: Prevent any exceptions from propagating to SWIG
            LOGGER.error(f"Critical error in onTransferFinish: {e}")
            self.continue_event.set()
            # Don't re-raise the exception to prevent SWIG director method error

    def onTransferTemporaryError(self, _api, transfer, error):
        try:
            filename = "Unknown"
            state = 0
            error_str = "Unknown error"

            try:
                if transfer:
                    filename = transfer.getFileName()
                    state = transfer.getState()
                if error:
                    error_str = error.toString()
            except Exception as e:
                LOGGER.warning(
                    f"Error getting transfer temporary error details: {e}"
                )

            LOGGER.warning(
                f"MEGA transfer temporary error for {filename}: {error_str} (state: {state}) - will retry"
            )

            # Most transfer temporary errors are recoverable and should be retried
            # Only cancel for truly unrecoverable states
            if state in [1, 2, 3, 4, 5]:  # Active transfer states that can recover
                LOGGER.warning(
                    f"MEGA transfer error is recoverable (state {state}), continuing..."
                )
                return

            # Only set error for truly unrecoverable states (6=failed, 7=cancelled)
            if state in [6, 7]:
                LOGGER.error(f"MEGA transfer failed permanently (state {state})")
                self.error = error_str
            else:
                # For unknown states, be conservative and allow retry
                LOGGER.warning(
                    f"MEGA transfer error for unknown state {state}, allowing retry"
                )
                return

            if not self.is_cancelled:
                self.is_cancelled = True
                try:
                    async_to_sync(
                        self.listener.on_download_error,
                        f"MEGA Transfer Error: {error_str} ({filename})",
                    )
                except Exception as e:
                    LOGGER.error(f"Error calling on_download_error: {e}")
                self.continue_event.set()
        except Exception as e:
            # Critical: Prevent any exceptions from propagating to SWIG
            LOGGER.error(f"Critical error in onTransferTemporaryError: {e}")
            # Don't re-raise the exception to prevent SWIG director method error

    async def cancel_download(self):
        self.is_cancelled = True
        await self.listener.on_download_error("Download Canceled by user")


class AsyncExecutor:
    def __init__(self):
        self.continue_event = Event()

    async def do(self, function, args, timeout=None):
        """Execute MEGA function with optional timeout"""
        getattr(function, "__name__", str(function))

        self.continue_event.clear()
        await sync_to_async(function, *args)

        if timeout is not None:
            await asyncio.wait_for(self.continue_event.wait(), timeout=timeout)
        else:
            await self.continue_event.wait()  # No timeout - wait indefinitely


async def _cleanup_mega_apis(api, folder_api, executor):
    """Helper function to cleanup MEGA API instances"""
    try:
        # Set the executor event to prevent any lingering timeouts
        if executor and hasattr(executor, "continue_event"):
            try:
                executor.continue_event.set()
            except Exception as e:
                LOGGER.warning(f"Error setting executor event: {e}")

        # Mark cleanup mode to ignore access denied errors during logout
        if (
            hasattr(executor, "continue_event")
            and hasattr(executor.continue_event, "listener")
            and hasattr(executor.continue_event.listener, "_cleanup_mode")
        ):
            executor.continue_event.listener._cleanup_mode = True

        # Logout directly without using executor to avoid timeout issues
        if api:
            try:
                # Use direct asyncio.wait_for instead of sync_to_async to avoid timeout issues
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, api.logout),
                    timeout=120,
                )
            except TimeoutError:
                LOGGER.warning(
                    "MEGA download logout timed out after 120 seconds, but download was successful"
                )
            except Exception as e:
                error_str = str(e).lower()
                if "access denied" in error_str:
                    LOGGER.warning(
                        f"Access denied during MEGA logout (expected): {e}"
                    )
                else:
                    LOGGER.warning(
                        f"MEGA download logout failed: {e}, but download was successful"
                    )

        if folder_api:
            try:
                # Use direct asyncio.wait_for for folder API logout too
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, folder_api.logout
                    ),
                    timeout=120,
                )
            except TimeoutError:
                LOGGER.warning(
                    "MEGA folder API logout timed out after 120 seconds, but download was successful"
                )
            except Exception as e:
                error_str = str(e).lower()
                if "access denied" in error_str:
                    LOGGER.warning(
                        f"Access denied during MEGA folder logout (expected): {e}"
                    )
                else:
                    LOGGER.warning(
                        f"MEGA folder API logout failed: {e}, but download was successful"
                    )
    except Exception as e:
        LOGGER.error(f"Error during MEGA API cleanup: {e}")


async def add_mega_download(listener, path):
    """
    Download files/folders from MEGA.nz using the MEGA SDK v4.8.0 with FFmpeg support.

    Args:
        listener: Download listener object
        path: Download destination path
    """
    # Check if MEGA operations are enabled
    if not Config.MEGA_ENABLED:
        await listener.on_download_error(
            "❌ MEGA.nz operations are disabled by the administrator."
        )
        return

    # Check if MEGA SDK v4.8.0 is available and working
    if not MEGA_SDK_AVAILABLE:
        error_msg = (
            "❌ MEGA SDK v4.8.0 not available. Please install megasdk v4.8.0."
        )
        if MEGA_IMPORT_ERROR:
            error_msg += f"\nImport error: {MEGA_IMPORT_ERROR}"
        await listener.on_download_error(error_msg)
        return

    # Check for user MEGA credentials first, then fall back to owner credentials
    user_mega_email = listener.user_dict.get("MEGA_EMAIL")
    user_mega_password = listener.user_dict.get("MEGA_PASSWORD")

    if user_mega_email and user_mega_password:
        MEGA_EMAIL = user_mega_email
        MEGA_PASSWORD = user_mega_password
    else:
        MEGA_EMAIL = Config.MEGA_EMAIL
        MEGA_PASSWORD = Config.MEGA_PASSWORD

    executor = AsyncExecutor()
    api = MegaApi(None, None, None, "aimleechbot")
    folder_api = None

    mega_listener = MegaAppListener(executor.continue_event, listener)
    api.addListener(mega_listener)

    try:
        # Login if credentials are provided
        if MEGA_EMAIL and MEGA_PASSWORD:
            # Validate credentials format before attempting login
            if not MEGA_EMAIL or not MEGA_PASSWORD:
                LOGGER.error("MEGA credentials are empty")
                await listener.on_download_error(
                    "❌ MEGA credentials not configured. Please set MEGA_EMAIL and MEGA_PASSWORD."
                )
                return

            if "@" not in MEGA_EMAIL or len(MEGA_EMAIL.split("@")) != 2:
                LOGGER.error(f"Invalid MEGA email format: {MEGA_EMAIL}")
                await listener.on_download_error(
                    "❌ Invalid MEGA email format. Please check your MEGA_EMAIL setting."
                )
                return

            if len(MEGA_PASSWORD) < 1:
                LOGGER.error("MEGA password is too short")
                await listener.on_download_error(
                    "❌ MEGA password appears to be empty. Please check your MEGA_PASSWORD setting."
                )
                return

            # Additional credential debugging for error -3 troubleshooting
            MEGA_EMAIL.split("@")

            # Check for common problematic characters
            problematic_chars = ["\n", "\r", "\t", " "]
            email_issues = [repr(c) for c in problematic_chars if c in MEGA_EMAIL]
            password_issues = [
                repr(c) for c in problematic_chars if c in MEGA_PASSWORD
            ]

            if email_issues:
                LOGGER.warning(
                    f"MEGA email contains problematic characters: {email_issues}"
                )
            if password_issues:
                LOGGER.warning(
                    f"MEGA password contains problematic characters: {password_issues}"
                )

            # Check for leading/trailing whitespace
            if MEGA_EMAIL.strip() != MEGA_EMAIL:
                LOGGER.warning("MEGA email has leading/trailing whitespace")
            if MEGA_PASSWORD.strip() != MEGA_PASSWORD:
                LOGGER.warning("MEGA password has leading/trailing whitespace")

            # Reset listener state before login
            mega_listener.error = None
            mega_listener.is_cancelled = False

            try:
                # Increase timeout to 180 seconds to handle slow MEGA servers
                await executor.do(
                    api.login, (MEGA_EMAIL, MEGA_PASSWORD), timeout=180
                )
                LOGGER.info("MEGA login successful")
            except TimeoutError:
                LOGGER.error("MEGA login timed out after 180 seconds")

                # Check if we had persistent error -3 during timeout
                if hasattr(mega_listener, "_retry_counts"):
                    error_3_count = mega_listener._retry_counts.get(
                        "1_-3", 0
                    )  # LOGIN type = 1
                    if error_3_count >= 10:
                        LOGGER.error(
                            f"MEGA login timed out with {error_3_count} error -3 retries - likely invalid credentials"
                        )
                        friendly_msg = (
                            "❌ MEGA Login Failed: Invalid credentials (persistent error -3).\n\n"
                            "After 180 seconds of retries, the login is still failing with error -3.\n"
                            "This strongly suggests:\n"
                            "• Incorrect MEGA email or password\n"
                            "• Email format issues (check for typos)\n"
                            "• Password encoding problems\n"
                            "• Account verification required\n\n"
                            "Please verify your credentials by logging into mega.nz manually."
                        )
                    else:
                        friendly_msg = (
                            "❌ MEGA Login Timeout: Connection took too long.\n\n"
                            "This usually means:\n"
                            "• MEGA servers are experiencing high load\n"
                            "• Network connectivity issues\n"
                            "• Firewall blocking MEGA connections\n\n"
                            "Please try again in a few minutes."
                        )
                else:
                    friendly_msg = (
                        "❌ MEGA Login Timeout: Connection took too long.\n\n"
                        "This usually means:\n"
                        "• MEGA servers are experiencing high load\n"
                        "• Network connectivity issues\n"
                        "• Firewall blocking MEGA connections\n\n"
                        "Please try again in a few minutes."
                    )

                await listener.on_download_error(friendly_msg)
                return
            except Exception as login_error:
                error_msg = str(login_error).lower()
                LOGGER.error(f"MEGA login failed: {login_error}")

                # Handle empty error messages (usually timeout-related)
                if not error_msg or error_msg.strip() == "":
                    LOGGER.warning(
                        "MEGA login failed with empty error message - likely timeout or connection issue"
                    )
                    # Check if listener has more specific error information
                    if mega_listener.error:
                        listener_error = str(mega_listener.error).lower()

                        if "invalid login credentials (error -3)" in listener_error:
                            friendly_msg = (
                                "❌ MEGA Login Failed: Invalid credentials (Error -3).\n\n"
                                "This usually means:\n"
                                "• Incorrect MEGA email or password\n"
                                "• Email format is invalid\n"
                                "• Password contains unsupported characters\n"
                                "• Account requires verification\n\n"
                                "Please double-check your MEGA credentials in bot settings."
                            )
                        elif "access denied" in listener_error:
                            friendly_msg = (
                                "❌ MEGA Login Failed: Access denied after timeout.\n\n"
                                "This usually means:\n"
                                "• Login took too long due to server issues\n"
                                "• Network connectivity problems\n"
                                "• MEGA servers are overloaded\n\n"
                                "Please try again in a few minutes."
                            )
                        else:
                            friendly_msg = (
                                "❌ MEGA Login Failed: Connection timeout.\n\n"
                                "This usually means:\n"
                                "• MEGA servers are experiencing high load\n"
                                "• Network connectivity issues\n"
                                "• Firewall blocking MEGA connections\n\n"
                                "Please try again in a few minutes."
                            )
                    else:
                        friendly_msg = (
                            "❌ MEGA Login Failed: Unknown connection issue.\n\n"
                            "This usually means:\n"
                            "• MEGA servers are experiencing issues\n"
                            "• Network connectivity problems\n"
                            "• Temporary service disruption\n\n"
                            "Please try again in a few minutes."
                        )
                # Check if this is a temporary network error
                elif any(
                    temp_error in error_msg
                    for temp_error in [
                        "request failed, retrying",
                        "temporary error",
                        "network error",
                        "connection timeout",
                        "error code: -1",
                        "error code: -2",
                        "error code: -3",
                        "error code: -4",
                        "error code: -5",
                    ]
                ):
                    LOGGER.warning(
                        f"MEGA login temporary error detected: {login_error}"
                    )
                    friendly_msg = (
                        "❌ MEGA Login Temporary Error: Network connectivity issue.\n\n"
                        "This usually means:\n"
                        "• Temporary network connectivity issues\n"
                        "• MEGA servers temporarily overloaded\n"
                        "• Rate limiting in effect\n\n"
                        "Please try again in a few minutes."
                    )
                # Provide specific error messages for common authentication issues
                elif any(
                    auth_error in error_msg
                    for auth_error in [
                        "access denied",
                        "invalid credentials",
                        "authentication failed",
                        "unauthorized",
                        "forbidden",
                        "login required",
                    ]
                ):
                    LOGGER.error(f"MEGA login authentication error: {login_error}")
                    friendly_msg = (
                        "❌ MEGA Authentication Failed: Invalid credentials.\n\n"
                        "This usually means:\n"
                        "• Incorrect MEGA email or password\n"
                        "• Account suspended or restricted\n"
                        "• Two-factor authentication enabled (not supported)\n\n"
                        "Please check your MEGA credentials in bot settings."
                    )
                elif "timed out" in error_msg:
                    LOGGER.warning(f"MEGA login timeout: {login_error}")
                    friendly_msg = "❌ MEGA login timed out. Please check your internet connection and try again."
                else:
                    LOGGER.error(f"MEGA login unknown error: {login_error}")
                    friendly_msg = f"❌ MEGA login failed: {login_error}"

                await listener.on_download_error(friendly_msg)
                return
        else:
            LOGGER.info("Using anonymous MEGA access (no credentials provided)")
            # Anonymous access

        # Check for login errors after authentication attempt
        # Note: After successful login, mega_listener.error may still contain the last temporary error
        # We need to check if login actually succeeded by verifying we can fetch nodes
        if mega_listener.error is not None:
            error_msg = str(mega_listener.error).lower()

            # Skip temporary network errors that should be retried or were resolved
            if any(
                temp_error in error_msg
                for temp_error in [
                    "request failed, retrying",
                    "temporary error",
                    "network error",
                    "connection timeout",
                    "error code: -1",
                    "error code: -2",
                    "error code: -3",
                    "error code: -4",
                    "error code: -5",
                    "invalid login credentials (error -3)",  # This was from temporary retries
                ]
            ):
                LOGGER.warning(
                    f"MEGA temporary network error during authentication (ignoring after successful login): {mega_listener.error}"
                )
                # Reset the error and allow the process to continue since login succeeded
                mega_listener.error = None
            else:
                # Only treat as real authentication error if it's not a temporary error
                LOGGER.error(
                    f"MEGA authentication error detected: {mega_listener.error}"
                )

                if any(
                    auth_error in error_msg
                    for auth_error in [
                        "access denied",
                        "invalid credentials",
                        "authentication failed",
                        "unauthorized",
                        "forbidden",
                        "login required",
                    ]
                ):
                    friendly_msg = (
                        "❌ MEGA Authentication Failed: Access denied.\n\n"
                        "This usually means:\n"
                        "• Invalid MEGA email or password\n"
                        "• Account suspended or restricted\n"
                        "• Two-factor authentication enabled (not supported)\n\n"
                        "Please check your MEGA credentials in bot settings."
                    )
                else:
                    friendly_msg = f"❌ MEGA Error: {mega_listener.error}"

                await listener.on_download_error(friendly_msg)
                return

        # Handle file vs folder links
        link_type = get_mega_link_type(listener.link)

        if link_type == "file":
            await executor.do(api.getPublicNode, (listener.link,), timeout=30)
            node = mega_listener.public_node
        else:
            folder_api = MegaApi(None, None, None, "aimleechbot")
            folder_api.addListener(mega_listener)
            await executor.do(folder_api.loginToFolder, (listener.link,), timeout=30)
            node = await sync_to_async(folder_api.authorizeNode, mega_listener.node)

        if mega_listener.error is not None:
            error_msg = f"Failed to get MEGA node: {mega_listener.error}"
            LOGGER.error(error_msg)
            await listener.on_download_error(error_msg)
            return

        if node is None:
            error_msg = "Failed to retrieve MEGA node"
            LOGGER.error(error_msg)
            await listener.on_download_error(error_msg)
            return

    except Exception as e:
        error_msg = f"Error accessing MEGA link: {e!s}"
        LOGGER.error(error_msg)
        await listener.on_download_error(error_msg)
        return
    finally:
        pass  # Cleanup will be done later after download completes

    # Set name and size based on the node type
    link_type = get_mega_link_type(listener.link)
    if link_type == "file":
        # For clone operations, preserve the original name stored by the clone process
        if hasattr(listener, "original_mega_name") and listener.original_mega_name:
            listener.name = listener.name or listener.original_mega_name
        else:
            listener.name = listener.name or node.getName()
        listener.size = node.getSize()
    else:
        # For clone operations, preserve the original name stored by the clone process
        if hasattr(listener, "original_mega_name") and listener.original_mega_name:
            listener.name = listener.name or listener.original_mega_name
        else:
            listener.name = listener.name or node.getName()
        listener.size = node.getSize()

        # For folders, try to get more accurate size information
        try:
            # Check if we can get child nodes for better size calculation
            if hasattr(node, "getChildren"):
                children = node.getChildren()
                if children:
                    actual_size = 0
                    child_count = 0
                    # MegaNodeList needs to be accessed by index, not iteration
                    if hasattr(children, "size"):
                        list_size = children.size()
                        for i in range(list_size):
                            try:
                                child = children.get(i)
                                if child and hasattr(child, "getSize"):
                                    child_size = child.getSize()
                                    child.getName() if hasattr(
                                        child, "getName"
                                    ) else f"Child_{i}"
                                    actual_size += child_size
                                    child_count += 1
                            except Exception as e:
                                LOGGER.error(f"Error accessing child {i}: {e}")

                        if actual_size > 0 and actual_size != listener.size:
                            LOGGER.warning(
                                f"Size mismatch - Node reports: {listener.size}, Calculated: {actual_size}"
                            )
                            # Use the calculated size instead of the wrong reported size
                            listener.size = actual_size
                    else:
                        # Fallback: try direct iteration
                        for child in children:
                            if hasattr(child, "getSize"):
                                child_size = child.getSize()
                                actual_size += child_size
                                child_count += 1
                        if actual_size > 0 and actual_size != listener.size:
                            LOGGER.warning(
                                f"Size mismatch - Node reports: {listener.size}, Calculated: {actual_size}"
                            )
                            listener.size = actual_size
        except Exception as e:
            LOGGER.error(f"Error analyzing folder structure: {e}")

    gid = token_hex(4)

    # Check size limits
    if listener.size > 0:
        limit_msg = await limit_checker(
            listener.size,
            listener,
            isTorrent=False,
            isMega=True,
            isDriveLink=False,
            isYtdlp=False,
        )
        if limit_msg:
            LOGGER.warning(f"MEGA download size limit exceeded: {limit_msg}")
            await listener.on_download_error(limit_msg)
            await _cleanup_mega_apis(api, folder_api, executor)
            return
        LOGGER.info("MEGA download size limit check passed")
    else:
        LOGGER.warning("MEGA download has zero size - skipping size limit check")

    # Check for duplicate downloads
    msg, button = await stop_duplicate_check(listener)
    if msg:
        LOGGER.info(f"MEGA download duplicate detected: {msg}")
        await listener.on_download_error(msg, button)
        await _cleanup_mega_apis(api, folder_api, executor)
        return
    LOGGER.info("MEGA download duplicate check passed")

    # Check if download should be queued
    added_to_queue, event = await check_running_tasks(listener)
    if added_to_queue:
        LOGGER.info("MEGA download added to queue")
        async with task_dict_lock:
            task_dict[listener.mid] = QueueStatus(listener, gid, "dl")
        await listener.on_download_start()
        await send_status_message(listener.message)
        LOGGER.info("Waiting for queue event...")
        await event.wait()
        async with task_dict_lock:
            if listener.mid not in task_dict:
                LOGGER.warning("MEGA download removed from task_dict while in queue")
                await _cleanup_mega_apis(api, folder_api, executor)
                return
        from_queue = True
        LOGGER.info("MEGA download started from queue")
    else:
        from_queue = False
        LOGGER.info("MEGA download will start immediately (not queued)")

    # Create download status tracker
    LOGGER.info("Creating MEGA download status tracker...")
    async with task_dict_lock:
        task_dict[listener.mid] = MegaDownloadStatus(
            listener.name, listener.size, gid, mega_listener, listener.message
        )

    # Start the download
    if from_queue:
        LOGGER.info("MEGA download starting from queue...")
    else:
        await listener.on_download_start()
        await send_status_message(listener.message)

    try:
        # Create download directory
        await makedirs(path, exist_ok=True)

        # Start the actual download with MEGA SDK v4.8.0 API signature
        # startDownload(node, localPath, customName, appData, startFirst, cancelToken)
        # localPath should be the full file path, not just the directory

        # Construct the full file path for the download
        file_path = f"{path}/{listener.name}"
        LOGGER.info(f"Starting MEGA download to: {file_path}")

        await executor.do(
            api.startDownload, (node, file_path, None, None, False, None)
        )

        LOGGER.info(
            "MEGA download started - waiting for all transfers to complete..."
        )

        # Wait for the download to complete
        # The mega_listener.continue_event will be set when transfer finishes
        await mega_listener.continue_event.wait()

        # Check if download completed successfully or with error
        if mega_listener.error:
            error_msg = f"MEGA download failed: {mega_listener.error}"
            LOGGER.error(error_msg)
            await listener.on_download_error(error_msg)
            return

        LOGGER.info("MEGA download completed successfully")

    except Exception as e:
        error_msg = f"Error during MEGA download: {e!s}"
        LOGGER.error(error_msg)
        await listener.on_download_error(error_msg)
    finally:
        # Always cleanup API connections
        # Set cleanup mode to suppress harmless logout errors
        if mega_listener:
            mega_listener._cleanup_mode = True
        await _cleanup_mega_apis(api, folder_api, executor)
