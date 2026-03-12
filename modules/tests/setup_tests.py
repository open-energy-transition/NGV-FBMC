from modules.analysis_toolkit.analyzer import ResultsComputer
from modules.analysis_toolkit.helpers.boundaries import get_fb_constraints


YEAR = 2030
RC = ResultsComputer(year=YEAR)
N = RC.ns.get_iem_dispatch()
T0 = RC.ns.get_iem_dispatch().snapshots[0]
PTDF = get_fb_constraints(YEAR)