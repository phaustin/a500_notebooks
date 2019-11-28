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
import re

matplotlib.use("Agg")

timestep = re.compile(r"

if __name__ == "__main__":
    data_dir = "/scratch/paustin/paustin/a500_data/small_bomex"
    zarr_in = list(data_dir.glob("*zarr"))
    print(all_timesteps)
    print(len(all_timesteps))
    full_name = all_timesteps[0].name
    ds_zarr = xr.open_zarr(str(all_timesteps[0]))
    ds_zarr_short = ds_zarr.isel(z=slice(0, 100), x=slice(0, 600), y=slice(0, 500))
    short_coords = list(ds_zarr.coords.items())
    varname_3d = ["QN", "QV", "TABS", "W", "TR01", "QR"]
    var_dict = {}
    for varname in varname_3d:
        print(varname)
        var_dict[varname] = ds_zarr_short[varname]
    varname_1d = ["p"]
    for varname in varname_1d:
        var_dict[varname] = ds_zarr_short[varname]
    ds_small = xarray.Dataset(var_dict)
    out_dir = nr.data_dir / "small_bomex"
    out_dir.mkdir(parents=True, exist_ok=True)
    parts = full_name.split("_")
    new_name = f"bomex_small_{parts[-1]}"
    full_path = out_dir / new_name
    ds_small.to_zarr(full_path, "w")
    print(f"wrote {full_path}")
