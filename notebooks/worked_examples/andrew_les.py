import xarray
from netCDF4 import Dataset
from pathlib import Path
import context
from datetime import datetime
from pytz import utc
import matplotlib
from matplotlib import pyplot as plt
import pprint
import xarray as xr
import pdb
matplotlib.use("Agg")
from a500.utils import ncdump
import re
#
# the following regular expression captures one group
# of exactly 3 characters, all numbers between 0-9
# the filenames look like ncep_gec00.t00z.pgrb2f006_SA.nc
#
find_hour = re.compile(r".*grb2.*(\d{3,3}).*_SA.nc")
#
#
#


def sort_hour(the_file):
    """
    sort the files by converting the 3 digit time to
    an integer and returning that number
    """
    the_match = find_hour.match(str(the_file))
    return int(the_match.group(1))


if __name__ == "__main__":
    zarr_in = list(context.data_dir.glob("*zarr"))[0]
    ds_zarr = xr.open_zarr(str(zarr_in))
    pdb.set_trace()
    # time_average = ds_big.mean('time')
    # #
    # # time_average.data_vars
    # # time_average.coords
    # varnames = list(ds_big.variables.keys())
    # #
    # #
    # # create an xarray out of these files
    # #
    # vel_vals = [
    #     'VVEL_200mb', 'VVEL_250mb', 'VVEL_500mb', 'VVEL_700mb', 'VVEL_925mb',
    #     'VVEL_1000mb'
    # ]
    # vel_dict = {}
    # for key in vel_vals:
    #     vel_dict[key] = ds_big.variables[key]
    # ds_small = xr.Dataset(vel_dict, ds_big.coords)
    # #
    # # select a slice for the first timestep
    # #
    # out = ds_small['VVEL_200mb'].isel(time=0, latitude=slice(2, 4), longitude=slice(2,4))
    # print(f"slice example {out}")
    # ds_small.attrs[
    #     'history'] = f"written by {str(context.this_dir / 'eve_xarray.py')}"
    # ds_small.to_zarr(context.data_dir / "small_wvel", 'w')
    # #
    # # make a plot and copy it to you public web site
    # #
    # plot_var = ds_small['VVEL_925mb'].isel(time=0)
    # plot_dir = Path().home() / "public_html" / "plot_dir"
    # plot_dir.mkdir(parents=True, exist_ok=True)
    # print(f"creating {plot_dir}")
    # fig, ax = plt.subplots(1, 1, figsize=(8, 8))
    # plot_name = "test.png"
    # ax.pcolormesh(ds_small.longitude,ds_small.latitude,plot_var)
    # ax.set(title = "VVEL_925mb", xlabel = "longitude", ylabel="latitude")
    # fig.savefig(plot_dir / plot_name)
