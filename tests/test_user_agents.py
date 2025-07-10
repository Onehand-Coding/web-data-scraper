from web_data_scraper.scraper.utils.user_agents import UserAgentManager


def test_user_agent_manager():
    manager = UserAgentManager()
    ua = manager.get_random_user_agent("desktop")
    assert ua in manager.user_agents["desktop"]
    ua_all = manager.get_random_user_agent()
    assert ua_all in [ua for agents in manager.user_agents.values() for ua in agents]
