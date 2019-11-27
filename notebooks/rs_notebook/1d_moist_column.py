# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
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

# %% [markdown] toc=true
# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"></ul></div>

# %%
import numpy as np
import scipy.integrate as integrate
#from matplotlib import pyplot as plt
from scipy.integrate import odeint

# %% [markdown]
# 1 column moist conceptual model

# %%
#
# Input paramters
#
Q_bl = [-1.0, -2.0, -3.0, -4.0, -5.0, -6.0] #K day^-1
Q_ft = -1.0 #K day^-1
Gamma = 5.0 #K km^-1
theta_0 = 298 #K
theta_sfc = 301 #K
A = 0.41
Cd = 0.001
V = 5 #m/s
tau = 15 #min
epsilon = 0.61

theta_sfc = 302 #K GUESS

w_ft = Q_ft/Gamma

# %% [markdown]
# Temperature profile at the boundary layer:
#
# $$\frac{\delta\theta_{BL}}{\delta\ t} = \ Q_{BL} + \frac{1}{h}*(\ w_{e}\Delta\theta +\ F_{\theta}) $$
#
#
# Moisture flux at the boundary layer:
#
# $$ \frac{\delta\ q_{BL}}{\delta\ t} = \frac{1}{h}*(\ w_{e}\Delta\ q +\ F_{q}) $$
#
# Change in BL height:
#
# $$ \frac{h}{\delta\ t} = \ w_{FT} + \ w_e + \ w_m $$

# %% [markdown]
# ![image.png](attachment:image.png)
# from https://journals.ametsoc.org/doi/pdf/10.1175/JAS-D-18-0226.1

# %%
import pdb
def equations_odeint(P,t):
    #
    #equations are in order of equations seen in figure above
    #
    pdb.set_trace()
    P[0] = Q_bl + (1/P[6])*P[10]*P[1] + P[2]
    P[1] = theta_0 + gamma*P[6] - P[0]
    P[2] = Cd*V*(theta_sfc - P[0])
    P[3] = (1/P[6])*(P[10]*P[4] + P[7])
    P[4] = q_ft - P[3] #fio what q_ft is
    P[5] = Cd*V*(q_sfc - P[3])
    P[6] = w_ft + P[10] + w_m
    P[7] = (theta_0 + gamma*P[6])*(1+epsilon*q_ft)-theta_v_bl #fio what theta_v_bl is
    P[8] = P[2] + epsilon*P[1]*P[5]
    P[9] = -P[6]-LCL #if LCL<h or 0 if greater
    
    P[10] = A*P[8]/P[7] #equation for w_e
    
    return(P[0], P[1], P[2], P[3], P[4], P[5], P[6], P[7], P[8],P[9], P[10])


# %%
#
# to start I have used the same P and t inputs as is splineprofiles.py
#

tf = 3*3600
dtout = 600
dz = 10.
ztop = 1000.

zh = np.arange(dz/2,(ztop-dz/2),dz)
thetai = 290 + 0.01*zh
tspan = np.arange(0.,tf,dtout)
zf = np.arange(0.,ztop,dz)

moist_profile=integrate.odeint(equations_odeint, thetai, tspan)

# %%
