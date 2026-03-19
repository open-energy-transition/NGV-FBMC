from enum import Enum

import matplotlib.colors as mcolors
from matplotlib.colors import LinearSegmentedColormap, ListedColormap


class Color(Enum):
    BLACK = ['#535F6B']
    WHITE = ['#FFFFFF']
    GREYS = ['#BFC3C7', '#EAEBEC', '#F0F0F0', '#F7F7F7']
    NSIDE_BLUE = ['#00ACC2', '#10C1D1', '#9FE0E8', '#DFF5F7']
    NSIDE_ORANGE = ['#F4A74F', '#F7BD7B', '#FBDEBD', '#FEF4F9']
    NSIDE_GREEN = ['#8CBB13', '#B7D46C', '#D4E5A7', '#F1F6E2']
    DARK_BLUE = ['#344CAF', '#6679C3', '#B3BCE1', '#E6E9F5']
    LIGHT_BLUE = ['#3384D0', '#66A3DC', '#B3D1ED', '#E5F0F9']
    PURPLE = ['#A83A8D', '#B36BAA', '#DEB5D4', '#F4E6F1']
    PINK = ['#D63487', '#E067A5', '#F0B3D2', '#FAE6F0']
    DARK_ORANGE = ['#E85F25', '#EE875C', '#F6C3AD', '#FCEBE4']
    DARK_GREEN = ['#009780', '#40B1A0', '#9FD8CF', '#DFF2EF']
    LIGHT_GREEN = ['#4BB24B', '#78C578', '#BCE2BC', '#E9F5E9']

    @classmethod
    def get_all_in_shade(cls, shade: int) -> list[str]:
        return [
            cls.NSIDE_BLUE.value[shade],
            cls.NSIDE_ORANGE.value[shade],
            cls.NSIDE_GREEN.value[shade],
            cls.DARK_BLUE.value[shade],
            cls.DARK_ORANGE.value[shade],
            cls.DARK_GREEN.value[shade],
            cls.LIGHT_BLUE.value[shade],
            cls.PURPLE.value[shade],
            cls.LIGHT_GREEN.value[shade],
            cls.PINK.value[shade],
        ]

    @classmethod
    def linear_colormap_in_color(cls, color: str, n: int=256) -> LinearSegmentedColormap:
        return LinearSegmentedColormap.from_list(f"{color} cmap", [cls.darken_color(color, 0.7), color], N=n)

    @classmethod
    def listed_colormap_in_color(cls, color: str, n: int) -> ListedColormap:
        return ListedColormap([cls.darken_color(color, 0.7), color], name="all colors cmap", N=n)

    @classmethod
    def _get_n_colors(cls, n: int) -> ListedColormap:
        return ListedColormap(cls.get_all_in_shade(0), name="all colors cmap", N=n)

    @classmethod
    def get_n_colors(cls, n: int) -> list[str]:
        return cls._get_n_colors(n).colors

    @classmethod
    def darken_color(cls, color: str, brightness: float) -> str:
        new_color = mcolors.to_rgb(color)
        new_color = tuple([c * brightness for c in new_color])
        return mcolors.to_hex(new_color)
