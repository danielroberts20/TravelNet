"""
upload/transaction/constants.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Lookup table mapping Wise internal account/pot identifiers (as they appear in
exported CSV filenames) to human-readable display names.

Keys are derived from the CSV filename pattern: <number>_<CURRENCY>
e.g. "137103728_USD" → "🇺🇸 USD".
"""

WISE_SOURCE_MAP = {
    "137103728_USD": "🇺🇸 USD",
    "137103780_AUD": "🇦🇺 AUD",
    "137103867_CAD": "🇨🇦 CAD",
    "138167086_AUD": "🇦🇺 Melbourne Fund",
    "148241577_USD": "🐲 South East Asia (🇺🇸)",
    "137103719_GBP": "🇬🇧 GBP",
    "138167566_NZD": "🇳🇿 NZD",
    "140828771_USD": "🇺🇸 US Travel",
    "147924418_EUR": "🇪🇺 EUR",
    "148241731_NZD": "🇳🇿 New Zealand Travel",
    "137103785_JPY": "[REMOVED] 🇯🇵 JPY",
    "148256355_THB": "[REMOVED] 🐲 South East Asia (🇹🇭)",
    "148256239_VND": "[REMOVED] 🐲 South East Asia (🇻🇳)",
    "140828737_USD": "[REMOVED] 🇦🇺 Australia East Coast (🇺🇸)",
    "149401853_AUD": "[REMOVED] 🇦🇺 Australia East Coast (🇦🇺)",
    "148084554_AUD": "[REMOVED] 🐲 South East Asia (🇦🇺)"
}
