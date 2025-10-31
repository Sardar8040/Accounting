"""Command registry for the Teleshop bot.

This module maintains a registry of all commands and their descriptions
to enable dynamic help generation and command discovery.
"""
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Command:
    """Represents a bot command and its metadata."""
    name: str  # Command name without leading slash
    description: str  # Human-readable description
    usage: Optional[str] = None  # Usage pattern if command takes arguments
    admin_only: bool = False  # Whether command requires admin privileges
    category: Optional[str] = None  # Optional grouping (e.g., "Inventory", "Reports")

# Global registry of all commands
_commands: List[Command] = []

def register_command(cmd: Command) -> None:
    """Register a new command in the global registry."""
    _commands.append(cmd)

def get_all_commands() -> List[Command]:
    """Get all registered commands."""
    return _commands.copy()

def get_commands_by_category(admin: bool = False) -> dict:
    """Get commands grouped by category, optionally filtering for admin commands."""
    categories = {}
    for cmd in _commands:
        if admin or not cmd.admin_only:
            cat = cmd.category or 'General'
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(cmd)
    return categories