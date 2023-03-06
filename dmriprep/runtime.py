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
import json
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


def get_sub_ses(path):
    """
    Give a list of path/sub-*/ses-* for all existing possibility from the
    directory path
    """
    sub_dirs = [d for d in os.listdir(path) if d.startswith("sub-")
                and os.path.isdir(os.path.join(path, d))]
    sub_ses_dirs = []
    for sub_dir in sub_dirs:
        for ses_dir in os.listdir(os.path.join(path, sub_dir)):
            if ses_dir.startswith("ses-") \
               and os.path.isdir(os.path.join(path, sub_dir, ses_dir)):
                sub_ses_dirs.append(os.path.join(path, sub_dir, ses_dir))
    return sub_ses_dirs


def run(datadir, outdir, simg_file, name="dmriprep",
        process=False, njobs=10, use_pbs=False, test=False):
    """ Parse data and execute the processing with hopla.

    Parameters
    ----------
    datadir: str
        path to the BIDS rawdata directory.
    outdir: str
        path to the BIDS derivatives directory.
    name: str, default 'dmriprep'
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
    list_sub_ses = get_sub_ses(datadir)
    list_dwi, list_bvec, list_bval, list_pe, list_readout, list_outdir = \
        [], [], [], [], [], []
    for sub_ses in list_sub_ses:
        print()
        print(sub_ses) # to delete
        # Verify all the mandatory file for prequal launch
        # dwi
        _dwi = glob.glob(os.path.join(sub_ses, "dwi",
                                      "*_acq-DWI*_run-*_dwi.nii.gz"))
        _dwi.sort()
        if len(_dwi) != 2:
            print(f"this sub and session don't have 2 dwi.nii.gz : "
                  f"'{sub_ses}'")
            continue
        dwi_files = ",".join(_dwi)

        # bvec
        _bvec = glob.glob(os.path.join(sub_ses, "dwi",
                                       "*_acq-DWI*_run-*_dwi.bvec"))
        _bvec.sort()
        if len(_bvec) != 2:
            print(f"this sub and session don't have 2 .bvec : "
                  f"'{sub_ses}'")
            continue
        bvec_files = ",".join(_bvec)

        # bval
        _bval = glob.glob(os.path.join(sub_ses, "dwi",
                                       "*_acq-DWI*_run-*_dwi.bval"))
        _bval.sort()
        if len(_bval) != 2:
            print(f"this sub and session don't have 2 .bval : "
                  f"'{sub_ses}'")
            continue
        bval_files = ",".join(_bval)
        # json
        _json = glob.glob(os.path.join(sub_ses, "dwi",
                                       "*_acq-DWI*_run-*_dwi.json"))
        _json.sort()

        if len(_json) != 2:
            print(f"this sub and session don't have 2 .json : "
                  f"'{sub_ses}'")
            continue

        # PhaseEncodingAxis and EstimatedTotalReadoutTime
        _pe = []
        _readout = []
        error_flag = False
        for file in _json:
            with open(file, 'r') as json_file:
                try:
                    # load the JSON data from the file
                    data = json.load(json_file)
                except json.decoder.JSONDecodeError as e:
                    error_flag = True
                    print(f"JSONDecodeError occured in {file}. Error "
                          f"message: {e}")
                except FileNotFoundError as e:
                    error_flag = True
                    print(f"FileNotFoundError occured in {file}. Error "
                          f"message: {e}")
            if "PhaseEncodingDirection" in data.keys():
                _pe.append(str(data["PhaseEncodingDirection"]))
            if "PhaseEncodingAxis" in data.keys():
                _pe.append(str(data["PhaseEncodingAxis"]))
            if "TotalReadoutTime" in data.keys():
                _readout.append(str(data["TotalReadoutTime"]))
            if "EstimatedTotalReadoutTime" in data.keys():
                _readout.append(str(data["EstimatedTotalReadoutTime"]))
        if error_flag is True:
            continue
        if len(_pe) != 2:
            print(f"this sub and session don't have 2 PhaseEncodingAxis "
                  f"values : '{sub_ses}'")
            continue
        pe_extracted = ",".join(_pe)
        if len(_readout) != 2:
            print(f"this sub and session don't have 2 "
                  f"EstimatedTotalReadoutTime values : '{sub_ses}'")
            continue
        readout_extracted = ",".join(_readout)

        # Outdir
        only_sub_ses = "/".join(sub_ses.split("/")[-2:])
        # _outdir = sub_ses.replace("rawdata", "derivatives/prequal")
        _outdir = os.path.join(outdir, name, only_sub_ses)
        if not os.path.isdir(_outdir):
            os.makedirs(_outdir)

        print(_outdir)
        # Append in list
        list_outdir.append(_outdir)
        list_dwi.append(dwi_files)
        list_bvec.append(bvec_files)
        list_bval.append(bval_files)
        list_pe.append(pe_extracted)
        list_readout.append(readout_extracted)
    if test:
        list_dwi = list_dwi[:1]
        list_bvec = list_bvec[:1]
        list_bval = list_bval[:1]
        list_pe = list_pe[:1]
        list_readout = list_readout[:1]
        list_outdir = list_outdir[:1]

    print(f"number of runs: {len(list_dwi)}")
    header = ["dwi", "bvec", "bval", "pe", "readout_time", "ouput_dir"]
    print("{:>8} {:>8} {:>8} {:>8}".format(*header))

    first = [item[0].replace(datadir, "").replace(outdir, "")
             for item in (list_dwi, list_bvec, list_bval, list_pe,
                          list_readout, list_outdir)]
    print("{:>8} {:>8} {:>8} {:>8}".format(*first))
    print("...")
    last = [item[-1].replace(datadir, "").replace(outdir, "")
            for item in (list_dwi, list_bvec, list_bval, list_pe,
                         list_readout, list_outdir)]
    print("{:>8} {:>8} {:>8} {:>8}".format(*last))
    if len(list_outdir) != len(list_dwi)\
       or len(list_outdir) != len(list_bvec)\
       or len(list_outdir) != len(list_bval)\
       or len(list_outdir) != len(list_readout)\
       or len(list_outdir) != len(list_pe):
        print("error, all the list are not the same size :")
        print(f"outdir : {len(list_outdir)}\n"
              f"dwi : {len(list_dwi)}\n"
              f"bvec : {len(list_bvec)}\n"
              f"bval : {len(list_bval)}\n"
              f"readout time : {len(list_readout)}\n"
              f"phase encoding direction : {len(list_pe)}")
        return 1
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
        cmd = (f"singularity run --bind {os.path.dirname(datadir)} "
               f"{simg_file} brainprep dmriprep")
        print(f"{cmd}") # to delete
        status, exitcodes = hopla(
            cmd,
            dwi=list_dwi,
            bvec=list_bvec,
            bval=list_bval,
            pe=list_pe,
            readout_time=list_readout,
            output_dir=list_outdir,
            # hopla_name_replace=True,
            hopla_iterative_kwargs=["dwi", "bvec", "bval",
                                    "pe", "readout_time", "output_dir"],
            hopla_optional=["dwi", "bvec", "bval",
                            "pe", "readout_time", "output_dir"],
            hopla_cpus=njobs,
            hopla_logfile=logfile,
            hopla_use_subprocess=True,
            hopla_verbose=1,
            hopla_python_cmd=None,
            **pbs_kwargs)


if __name__ == "__main__":
    fire.Fire(run)
