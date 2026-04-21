from typing import Literal

# PyPSA statistics
GROUPBY_OPTIONS = Literal['component', 'carrier', 'name', 'bus', 'bus_carrier', 'country']
GLOBAL_GROUPBY = ["carrier", "name", "bus", "country"]
GROUPBY_TIME = False

# PLOTTING
FIG_SIZE = (8, 3)
COLORS = {
        'blue': '#00ACC2',
        'green': '#8CBB13',
        'orange': '#F4A74F'
    }

_CAPTURE_RATE_COUNTRY = {
    'FR': 0.76,
    'BE': 0.671,
    'NL': 0.653,
    'DK': 0.818,
    'DE': 0.725,
    'NO': 1.0,
    'IE': 1.0,
    'GBNI': 1.0,
}

CAPTURE_RATE_IC = {
    'Aminth': _CAPTURE_RATE_COUNTRY['DK'],
    'BritNed': _CAPTURE_RATE_COUNTRY['NL'],
    'Continental Link': _CAPTURE_RATE_COUNTRY['NO'],
    'Cronos': _CAPTURE_RATE_COUNTRY['BE'],
    'East-West': _CAPTURE_RATE_COUNTRY['IE'],
    'ElecLink': _CAPTURE_RATE_COUNTRY['FR'],
    'FAB Link': _CAPTURE_RATE_COUNTRY['FR'],
    'Greenlink (Greenwire)': _CAPTURE_RATE_COUNTRY['IE'],
    'Gridlink': _CAPTURE_RATE_COUNTRY['FR'],
    'IFA': _CAPTURE_RATE_COUNTRY['FR'],
    'IFA2': _CAPTURE_RATE_COUNTRY['FR'],
    'Kulizumboo': _CAPTURE_RATE_COUNTRY['FR'],
    'LirIC': _CAPTURE_RATE_COUNTRY['IE'],
    'MARES': _CAPTURE_RATE_COUNTRY['IE'],
    'Moyle': _CAPTURE_RATE_COUNTRY['GBNI'],
    'NS Link (NSL)': _CAPTURE_RATE_COUNTRY['NO'],
    'Nautilus': _CAPTURE_RATE_COUNTRY['BE'],
    'Nemo': _CAPTURE_RATE_COUNTRY['BE'],
    'NeuConnect': _CAPTURE_RATE_COUNTRY['DE'],
    'NorthConnect': _CAPTURE_RATE_COUNTRY['NO'],
    'SENECA': _CAPTURE_RATE_COUNTRY['NL'],
    'Viking Link': _CAPTURE_RATE_COUNTRY['DK']
}