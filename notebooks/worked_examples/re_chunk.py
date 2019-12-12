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

# %%
timestep = re.compile(r".*_(\d+).zarr")
cluster = LocalCluster(10)
client = Client(cluster)

def sort_step(filename):
    out = str(filename)
    the_match = timestep.match(out)
    if the_match:
        the_step = int(the_match.group(1))
    else:
        the_step = None
    return the_step


# %%
client

# %%

data_dir = context.data_dir / Path("small_bomex")
zarr_in = list(data_dir.glob("*zarr"))
zarr_in.sort(key=sort_step)
print(zarr_in[:10])
zarr_list = []
the_time = 0
#
# open each zarr file as an xarray dataset
# correcting the timestamp

# %%
for ds_zarr in zarr_in:
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
# compute the mean and perturbation for timestep 0 and
# write out as a new zarr file
#
    

# %%
zarr_time_ds

# %%
zarr_mean = zarr_time_ds.mean(dim=('x','y'))
zarr_mean

# %%
zarr_time_ds["TABS"].data

# %%
net_tabs = zarr_time_ds["TABS"].chunk((1,100,500,600))
net_tabs.data

# %%
net_tabs


# %%
def calc_perturb(the_array):
    the_mean  = the_array.mean(dim=('x', 'y'))
    the_perturb = the_array - the_mean
    return the_perturb

vars = ['TABS','W','TR01','QV']
perturb_dict = {}
for a_var in vars:
    perturb_dict[a_var] = \
    xr.map_blocks(calc_perturb, zarr_time_ds[a_var][0,...].chunk((100,500,600)))

# %%
zarr_time_ds['TABS']

# %%
out = xr.map_blocks(calc_perturb, net_tabs[0,...])
out.data

# %%
out2 = calc_perturb(net_tabs[0,...])
out2.data

# %%
perturb_dict['TABS'].data

# %%
time_step = 0
print(f"finding perturbation for  {zarr_in[time_step]}")
temp = zarr_time_ds['TABS']
wvel = zarr_time_ds['W']
tr01 = zarr_time_ds['TR01']
mean_temp = temp[time_step, :, :, :].mean(dim=('x', 'y'))
mean_w = wvel[time_step, :, :, :].mean(dim=('x', 'y'))
mean_tr = tr01[time_step, :, :, :].mean(dim=('x', 'y'))
w_prime = wvel - mean_w
T_prime = temp - mean_temp
tr_prime = tr01 - mean_tr
#
# put these variables in a dictionary so they can be placed
# in a new dataset
#
varnames = [
    'mean_temp', 'mean_w', 'mean_tr', 'w_prime', 'T_prime', 'tr_prime'
]
ds_vec = [mean_temp, mean_w, mean_tr, w_prime, T_prime, tr_prime]
new_ds = xr.Dataset(dict(zip(varnames, ds_vec)))

# %%
#
# keep this small for illustration purposes
#
new_ds_slim = new_ds.isel(x=slice(0, 40), y=slice(0, 50))
#
# make the timestep from the original file part of the name.
#
parts = str(zarr_in[time_step]).split("_")
user_name = os.environ['USER']
out_dir = context.data_dir / Path(f"flux_bomex")
out_dir.mkdir(parents=True, exist_ok=True)
full_path = out_dir / f"flux_{parts[-1]}"
#
# the next step is important!!!  if you write out the ds
# without first computing, it will write the entire multi-timestep file
#
#
new_ds_slim.compute()
new_ds_slim.to_zarr(full_path, "w")
print(f"wrote {full_path}")

# %%
if __name__ == "__main__":
    client = 
    data_dir = context.data_dir / Path("small_bomex")
    zarr_in = list(data_dir.glob("*zarr"))
    zarr_in.sort(key=sort_step)
    print(zarr_in[:10])
    zarr_list = []
    the_time = 0
    #
    # open each zarr file as an xarray dataset
    # correcting the timestamp
    for ds_zarr in zarr_in:
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
    # compute the mean and perturbation for timestep 0 and
    # write out as a new zarr file
    #
    time_step = 0
    print(f"finding perturbation for  {zarr_in[time_step]}")
    temp = zarr_time_ds['TABS']
    wvel = zarr_time_ds['W']
    tr01 = zarr_time_ds['TR01']
    mean_temp = temp[time_step, :, :, :].mean(dim=('x', 'y'))
    mean_w = wvel[time_step, :, :, :].mean(dim=('x', 'y'))
    mean_tr = tr01[time_step, :, :, :].mean(dim=('x', 'y'))
    w_prime = wvel - mean_w
    T_prime = temp - mean_temp
    tr_prime = tr01 - mean_tr
    #
    # put these variables in a dictionary so they can be placed
    # in a new dataset
    #
    varnames = [
        'mean_temp', 'mean_w', 'mean_tr', 'w_prime', 'T_prime', 'tr_prime'
    ]
    ds_vec = [mean_temp, mean_w, mean_tr, w_prime, T_prime, tr_prime]
    new_ds = xr.Dataset(dict(zip(varnames, ds_vec)))
    #
    # keep this small for illustration purposes
    #
    new_ds_slim = new_ds.isel(x=slice(0, 40), y=slice(0, 50))
    #
    # make the timestep from the original file part of the name.
    #
    parts = str(zarr_in[time_step]).split("_")
    user_name = os.environ['USER']
    out_dir = context.data_dir / Path(f"flux_bomex")
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / f"flux_{parts[-1]}"
    


# %%
repr(new_ds)


# %%    #
    new_ds_slim.compute()
    new_ds_slim.to_zarr(full_path, "w")
    print(f"wrote {full_path}")


# %%
repr(new_ds)


# %%
