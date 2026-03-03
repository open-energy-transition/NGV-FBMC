from typing import Literal

# PyPSA statistics
GROUPBY_OPTIONS = Literal['component', 'carrier', 'name', 'bus', 'bus_carrier', 'country', 'time']
GLOBAL_GROUPBY = ["carrier", "name", "bus"]

# PLOTTING
FIG_SIZE = (8, 3)
COLORS = {
        'blue': '#00ACC2',
        'green': '#8CBB13',
        'orange': '#F4A74F'
    }