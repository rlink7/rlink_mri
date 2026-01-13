# -*- coding: utf-8 -*-
##########################################################################
# NSAp - Copyright (C) CEA, 2023
# Distributed under the terms of the CeCILL-B license, as published by
# the CEA-CNRS-INRIA. Refer to the LICENSE file or to
# http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
# for details.
##########################################################################


# Imports
import os
import fire
import glob
import datetime
import collections
import pandas as pd
from hopla.converter import hopla
import limri


def run(datadir, outdir, phdir, participant_file, name="li2mninorm",
        process=False, njobs=10, use_pbs=False, cmd="limri", test=False):
    """ Parse data and execute the processing with hopla.

    Parameters
    ----------
    datadir: str
        path to the li2mni derivatives directory.
    outdir: str
        path to the BIDS derivatives directory.
    phdir: str
        path to the phantom directory.
    participant_file: str
        path to the participants.tsv file.
    name: str, default 'li2mninorm'
        the name of the cirrent analysis.
    process: bool, default False
        optionnaly launch the process.
    njobs: int, default 10
        the number of parallel jobs.
    use_pbs: bool, default False
        optionnaly use PBSPRO batch submission system.
    cmd: str, default 'limri'
        the command to execute.
    test: bool, default False
        optionnaly, select only one subject.
    """
    files = glob.glob(os.path.join(
        datadir, "sub-*", "ses-M03Li", "li2mni.nii.gz"))
    info = pd.read_csv(participant_file, sep="\t")
    mask_file = os.path.join(
        os.path.dirname(limri.__file__), "resources",
        "MNI152_T1_2mm_brain_mask.nii.gz")
    assert os.path.isfile(mask_file), mask_file
    ph_file = os.path.join(phdir, "phantom_mean_value.tsv")
    ph_df = pd.read_csv(ph_file, sep="\t", dtype=str)
    ph_ref_vals = dict((_site, _mean) for _site, _mean in zip(
        ph_df.Site.values, ph_df.Mean._values))
    li_files, ph_vals, sub_outdirs = [], [], []
    for path in files:
        _outdir = os.path.dirname(path)
        norm_file = os.path.join(_outdir, "li2mninorm.nii.gz")
        if os.path.isfile(norm_file):
            os.remove(norm_file)
            continue
        _sid = path.split(os.sep)[-3]
        _info = info.loc[info["participant_id"] == _sid]
        _center = int(_info["ses-M03Li_center"].item())
        assert _center in [1, 2, 4, 5, 10, 11, 15], f"{_sid} - {_center}"
        _ph_val = ph_ref_vals[str(_center)]
        li_files.append(path)
        ph_vals.append(_ph_val)
        sub_outdirs.append(_outdir)
    if len(li_files) == 0:
        raise RuntimeError("No data to process!")
    if test:
        li_files = li_files[:1]
        ph_vals = ph_vals[:1]
        sub_outdirs = sub_outdirs[:1]
    print(f"number of runs: {len(li_files)}")
    header = ["li", "ph", "outdir"]
    print("{:>8} {:>8} {:>8}".format(*header))
    first = [item[0].replace(datadir, "").replace(phdir, "")
             for item in (li_files, ph_vals, sub_outdirs)]
    print("{:>8} {:>8} {:>8}".format(*first))
    print("...")
    last = [item[-1].replace(datadir, "").replace(phdir, "")
            for item in (li_files, ph_vals, sub_outdirs)]
    print("{:>8} {:>8} {:>8}".format(*last))

    if process:
        pbs_kwargs = {}
        if use_pbs:
            clusterdir = os.path.join(outdir, f"{name}_pbs")
            if not os.path.isdir(clusterdir):
                os.makedirs(clusterdir)
            pbs_kwargs = {
                "hopla_cluster": True,
                "hopla_cluster_logdir": clusterdir,
                "hopla_cluster_queue": "Nspin_long"}
        date = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        logdir = os.path.join(outdir, "logs")
        if not os.path.isdir(logdir):
            os.makedirs(logdir)
        logfile = os.path.join(logdir, f"{name}_{date}.log")
        status, exitcodes = hopla(
            "li2mninorm",
            li2mni_file=li_files,
            mask_file=mask_file,
            outdir=sub_outdirs,
            norm="norm",
            ref_value=ph_vals,
            hopla_iterative_kwargs=["li2mni_file", "ref_value", "outdir"],
            hopla_optional=["li2mni_file", "ref_value", "mask_file",
                            "outdir", "norm"],
            hopla_cpus=njobs,
            hopla_logfile=logfile,
            hopla_use_subprocess=True,
            hopla_verbose=1,
            hopla_python_cmd=cmd if os.path.isfile(cmd) else "",
            **pbs_kwargs)


if __name__ == "__main__":
    fire.Fire(run)
