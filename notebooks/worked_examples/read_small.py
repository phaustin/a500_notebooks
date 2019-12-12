# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     notebook_metadata_filter: all,-language_info,-toc,-latex_envs
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
''''
demo
'''
import xarray
from pathlib import Path
import xarray as xr
import re
import os
import context
from dask.distributed import Client, LocalCluster
timestep = re.compile(r".*_(\d+).zarr")


def sort_step(filename):
    out = str(filename)
    the_match = timestep.match(out)
    if the_match:
        the_step = int(the_match.group(1))
    else:
        the_step = None
    return the_step


zarr_in = list(context.small_bomex.glob("*zarr"))
zarr_in.sort(key=sort_step)
print(zarr_in[:10])
#
# open each zarr file as an xarray dataset
# correcting the timestamp

# %%
the_time = 0
zarr_list = []
tlim = 5
for ds_zarr in zarr_in[:tlim]:
    ds = xr.open_zarr(str(ds_zarr))
    #
    # wrote incorrect times in original files, fix here
    #
    ds['time'] = the_time
    zarr_list.append(ds)
    the_time += 60
#
# make a virtual dataset with time as the outer dimension
#
zarr_time_ds = xr.combine_nested(zarr_list, 'time')
#
# now subset this
# 
xlim = 40
ylim = 50
zlim = 60
zarr_slim_ds = zarr_time_ds.isel(x=slice(0, xlim),
                                 y=slice(0, ylim),
                                 z=slice(0, zlim))
#
# we'll want to save the mean and perturbation
#

def calc_mean(the_array):
    the_mean = the_array.mean(dim=('x', 'y'))
    return the_mean


def calc_perturb(the_array):
    the_mean = calc_mean(the_array)
    the_perturb = the_array - the_mean
    return the_perturb
#
# save the perturbation and mean in separate dictionaries
#
vars = ['TABS', 'W', 'TR01']
perturb_dict_keys = ['temp_prime', 'w_prime', 'tr_prime']
mean_dict_keys = ['temp_mean', 'w_mean', 'tr_mean']
perturb_dict = {}
key_pairs = zip(perturb_dict_keys, vars)
for key, a_var in key_pairs:
    perturb_dict[key] = \
      xr.map_blocks(calc_perturb, zarr_slim_ds[a_var].chunk((tlim,zlim,ylim,xlim)))

mean_dict = {}
key_pairs = zip(mean_dict_keys, vars)
for a_var in vars:
    mean_dict[a_var] = \
      xr.map_blocks(calc_mean, zarr_slim_ds[a_var].chunk((tlim,zlim,ylim,xlim)))
#
# now make a new dataset with these variables
#
    
new_ds = xr.Dataset(perturb_dict)
#
# and add the remaining means
#
for key, value in mean_dict.items():
    new_ds[key] = value

zarr_out = context.scratch_dir / "testout2.zarr"

new_ds.compute()
new_ds.to_zarr(zarr_out,'w')
#
# zarr bug -- change permissions so that
# everyone can read:  https://www.linode.com/docs/tools-reference/tools/modify-file-permissions-with-chmod/
# should be unnecessary after https://github.com/zarr-developers/zarr-python/pull/493
#
all_files=zarr_out.glob("**/*")
[item.chmod(0o755) for item in all_files]

    
