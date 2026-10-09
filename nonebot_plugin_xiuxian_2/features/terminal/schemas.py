"""Durable bounds of the Web terminal owner.

The terminal hands an authenticated operator an interactive shell, so its
contract is made of the values that keep that hand-off predictable: where the
authorization secret is read, what a granted session may keep and for how long,
the exact child-process and PTY environment, the read/poll/wait numbers of the
streaming loop, and the end-of-stream notice the browser relies on.

``TERMINAL_PASSWORD_ENVIRONMENT_VARIABLE`` is deliberately not a ``ConfigSpec``.
A declared config key would surface the secret in the configuration panel and in
config exports, while the code treats it as process configuration that is popped
out of the child environment before the shell starts.
"""

from __future__ import annotations


TERMINAL_PASSWORD_ENVIRONMENT_VARIABLE = "XIUXIAN_WEB_TERMINAL_PASSWORD"
AUTHORIZATION_SESSION_KEY = "terminal_authorized_until"
AUTHORIZATION_TTL_SECONDS = 300
PTY_SHELL_ARGUMENTS = ("/bin/bash", "--login", "-i")
PTY_TERMINAL_TYPE = "xterm-256color"
PTY_LANGUAGE = "zh_CN.UTF-8"
PTY_PROMPT = r"\[\033[01;32m\]\u\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ "
PTY_READ_BYTES = 16 * 1024
PTY_SELECT_TIMEOUT_SECONDS = 0.5
PTY_SESSION_TERMINATED_NOTICE = "\n[Session Terminated]\n"
PROCESS_REAP_WAIT_SECONDS = 1
PROCESS_POLL_WAIT_SECONDS = 0
WORKING_DIRECTORY_LINK_TEMPLATE = "/proc/{pid}/cwd"
UNKNOWN_WORKING_DIRECTORY = "~"

__all__ = [
    "AUTHORIZATION_SESSION_KEY",
    "AUTHORIZATION_TTL_SECONDS",
    "PROCESS_POLL_WAIT_SECONDS",
    "PROCESS_REAP_WAIT_SECONDS",
    "PTY_LANGUAGE",
    "PTY_PROMPT",
    "PTY_READ_BYTES",
    "PTY_SELECT_TIMEOUT_SECONDS",
    "PTY_SESSION_TERMINATED_NOTICE",
    "PTY_SHELL_ARGUMENTS",
    "PTY_TERMINAL_TYPE",
    "TERMINAL_PASSWORD_ENVIRONMENT_VARIABLE",
    "UNKNOWN_WORKING_DIRECTORY",
    "WORKING_DIRECTORY_LINK_TEMPLATE",
]
