import xarray
from pathlib import Path
import context
import matplotlib
from matplotlib import pyplot as plt
import pprint
import xarray as xr
import pdb
matplotlib.use("Agg")



if __name__ == "__main__":
    zarr_in = list(context.data_dir.glob("*zarr"))[0]
    ds_zarr = xr.open_zarr(str(zarr_in))
    pdb.set_trace()
=======

if __name__ == "__main__":
    # zarr_in = list(context.data_dir.glob("*zarr"))[0]
    all_timesteps = list(
        Path("/Howard16TB/data/loh/BOMEX_SWAMP/variables").glob("*.zarr"))[:10]
    ds_zarr = xr.open_zarr(str(all_timesteps[0]))
    ds_zarr_short = ds_zarr.isel(z=slice(0, 100),
                                 x=slice(0, 600),
                                 y=slice(0, 500))
    varname_3d = ['QN', 'QV', 'TABS']
    var_dict = {}
    for varname in varname_3d:
        print(varname)
        the_var = ds_zarr.variables[varname].isel(z=slice(0, 100),
                                                x=slice(0, 600),
                                                y=slice(0, 500))
        var_dict[varname] = the_var
    varname_1d = ['p']
    for varname in varname_1d:
        the_var = ds_zarr.variables[varname].isel(z=slice(0, 100))
        var_dict[varname] = the_var
    var_dict['time'] = time
    
>>>>>>> checkpoint
