from config import FILTERS

def get_alert_level(size_usd):
    """
    Return the filter config (emoji, name) if size_usd >= threshold.
    Returns None if no filter matches.
    """
    # Filters are ordered by size desc in config?
    # Let's sort them just in case.
    sorted_filters = sorted(FILTERS, key=lambda x: x['min'], reverse=True)
    
    for f in sorted_filters:
        if size_usd >= f['min']:
            return f
    return None
