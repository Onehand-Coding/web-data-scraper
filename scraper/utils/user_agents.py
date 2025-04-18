"""
User agent string management and rotation.
"""

from typing import List
import random

# Common user agents for different browsers and devices
USER_AGENTS = {
    'desktop': [
        # Chrome
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',

        # Firefox
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',

        # Safari
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
    ],
    'mobile': [
        # Android
        'Mozilla/5.0 (Linux; Android 10; SM-G980F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',

        # iPhone
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
    ],
    'tablet': [
        # iPad
        'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',

        # Android Tablet
        'Mozilla/5.0 (Linux; Android 8.0.0; SM-T830) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Safari/537.36'
    ]
}

class UserAgentManager:
    """Manages user agent strings for requests."""

    def __init__(self, agents: Dict = None):
        """Initialize with custom user agents if provided."""
        self.user_agents = agents or USER_AGENTS

    def get_random_user_agent(self, device_type: str = None) -> str:
        """Get a random user agent string."""
        if device_type and device_type in self.user_agents:
            return random.choice(self.user_agents[device_type])
        else:
            # Combine all user agents if no specific type requested
            all_agents = []
            for agents in self.user_agents.values():
                all_agents.extend(agents)
            return random.choice(all_agents)

    def get_user_agents(self, device_type: str = None) -> List[str]:
        """Get list of user agents for a specific device type."""
        if device_type and device_type in self.user_agents:
            return self.user_agents[device_type]
        else:
            all_agents = []
            for agents in self.user_agents.values():
                all_agents.extend(agents)
            return all_agents
