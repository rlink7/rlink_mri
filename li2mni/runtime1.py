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
from hopla.converter import hopla


def get_best_anat(files):
    """ Select the best anat file.
    """
    if len(files) == 1:
        return files[0]
    elif len(files) > 1:
        select = [path for path in files if "yGC" in path]
        assert len(select) == 1, files
        return select[0]
    else:
        raise ValueError("No anatomical file provided!")


def run(datadir, outdir, name="li2mni", process=False, njobs=10,
        use_pbs=False, cmd="limri", test=False):
    """ Parse data and execute the processing with hopla.

    Parameters
    ----------
    datadir: str
        path to the BIDS rawdata directory.
    outdir: str
        path to the BIDS derivatives directory.
    name: str, default 'li2mni'
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
        datadir, "sub-*", "ses-M03Li", "lithium",
        "sub-*_ses-M03Li_*part-mag_limri.nii.gz"))
    subjects = [path.split(os.sep)[-4] for path in files]
    duplicates = [
        item for item, count in collections.Counter(subjects).items()
        if count > 1]
    if len(duplicates) > 0:
        print(duplicates)
        raise RuntimeError("Multiple Lithium images for one subject no "
                           "allowed!")
    li_files, lianat_files, hanat_files, sub_outdirs = [], [], [], []
    for path in files:
        subject = path.split(os.sep)[-4]
        _outdir = os.path.join(outdir, name, subject, "ses-M03Li")
        _status_file = os.path.join(_outdir, "li2mni.nii.gz")
        if os.path.isfile(_status_file):
            continue
        _lianat_files = glob.glob(os.path.join(
            datadir, subject, "ses-M03Li", "anat",
            f"{subject}_ses-M03Li_*T1w.nii.gz"))
        sesdir = os.path.join(os.path.join(datadir, subject, "ses-M03"))
        if not os.path.isdir(sesdir):
            print(f"no '{sesdir}' session available: {sesdir}")
            continue
        if not os.path.isdir(_outdir):
            os.makedirs(_outdir)
        _hanat_files = glob.glob(os.path.join(
            sesdir, "anat", f"{subject}_ses-M03_*T1w.nii.gz"))
        li_files.append(path)
        lianat_files.append(get_best_anat(_lianat_files))
        hanat_files.append(get_best_anat(_hanat_files))
        sub_outdirs.append(_outdir)
    if len(li_files) == 0:
        raise RuntimeError("No data to process!")
    if test:
        li_files = li_files[:1]
        lianat_files = lianat_files[:1]
        hanat_files = hanat_files[:1]
        sub_outdirs = sub_outdirs[:1]
    print(f"number of runs: {len(li_files)}")
    header = ["li", "lianat", "hanat", "outdir"]
    print("{:>8} {:>8} {:>8} {:>8}".format(*header))
    first = [item[0].replace(datadir, "").replace(outdir, "")
             for item in (li_files, lianat_files, hanat_files, sub_outdirs)]
    print("{:>8} {:>8} {:>8} {:>8}".format(*first))
    print("...")
    last = [item[-1].replace(datadir, "").replace(outdir, "")
            for item in (li_files, lianat_files, hanat_files, sub_outdirs)]
    print("{:>8} {:>8} {:>8} {:>8}".format(*last))

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
            "li2mni",
            li_file=li_files,
            lianat_file=lianat_files,
            hanat_file=hanat_files,
            outdir=sub_outdirs,
            hopla_iterative_kwargs=["li_file", "lianat_file", "hanat_file",
                                    "outdir"],
            hopla_optional=["li_file", "lianat_file", "hanat_file",
                            "outdir"],
            hopla_cpus=njobs,
            hopla_logfile=logfile,
            hopla_use_subprocess=True,
            hopla_verbose=1,
            hopla_python_cmd=cmd if os.path.isfile(cmd) else "",
            **pbs_kwargs)


if __name__ == "__main__":
    fire.Fire(run)
