# -*- coding: utf-8 -*-
##########################################################################
# NSAp - Copyright (C) CEA, 2023
# Distributed under the terms of the CeCILL-B license, as published by
# the CEA-CNRS-INRIA. Refer to the LICENSE file or to
# http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
# for details.
##########################################################################


# Imports
import fire
import os
from nilearn import plotting
from PIL import Image
import shutil
import pandas as pd


def overlay_nifti(template_file, overlay_file, output_file,
                  cmap='jet', dpi=900):
    sub = template_file.split("sub-")[1].split("/ses")[0]
    display = plotting.plot_anat(template_file, display_mode="ortho",
                                 cut_coords=(-35, -18, -38),
                                 title=f"sub-{sub}")
    display.add_overlay(overlay_file, colorbar=True,
                        cmap=cmap,
                        alpha=0.4)
    display.savefig(output_file, dpi=dpi)
    display.close()


def cohorte(li2mni_path, output_path, norm=False, site=False,
            participants=None, pdf=True, dpi=900):
    """ Launch overlay_nifti on a all cohorte.

    Parameters
    ----------
    li2mni_path: str
        path to the li2mni folder.
    output_path: str
        path to the destination folder.

    Optionnal
    ---------
    norm: bool default False
        take the li2mni output normalized or not.
    site: bool default False
        Reorder the png to a site by site.
    participants: Bool default None
        path to the participants.tsv file. WARNING this flags is mandatory if
        site=True
    pdf: bool
        make a concatenation pdf of site by site png, work only if site=True
    dpi: int default=900
        dot per inch of the png created.
    """
    list_sub = [i for i in os.listdir(li2mni_path) if i.startswith('sub')]
    print(list_sub)
    print(len(list_sub))
    for sub in list_sub:
        template = f"{li2mni_path}/{sub}/ses-M03Li/lianat2mni.nii.gz"
        assert os.path.isfile(template), f"file not found : {template}"
        if norm is False:
            overlay = f"{li2mni_path}/{sub}/ses-M03Li/li2mni.nii.gz"
            output = os.path.join(output_path, f"{sub}_overlay_mni")
            assert os.path.isfile(template), f"file not found : {overlay}"
        else:
            overlay = f"{li2mni_path}/{sub}/ses-M03Li/li2mninorm.nii.gz"
            assert os.path.isfile(template), f"file not found : {overlay}"
            output = os.path.join(output_path, f"{sub}_overlay_mni_norm")

        if os.path.isfile(template) and os.path.isfile(overlay)\
           and os.path.isfile(output + ".png") is False:
            print(f"Making {sub}")
            print(os.path.isfile(output))
            print(output)
            overlay_nifti(template, overlay, output, dpi=dpi)

    if site is True and participants is not None:
        participants = pd.read_csv(participants, sep="\t")
        path = output_path
        list_png = [f for f in os.listdir(path) if f.startswith("sub-")]
        liste_site = []
        for png in list_png:
            sub = png.split("_")[0]
            site = int(participants["ses-M00_center"]
                       [participants["participant_id"] == sub].values[0])
            site = str(site)
            os.makedirs(os.path.join(path, f"site-{site}"), exist_ok=True)

            file_to_check = os.path.join(path,
                                         f"site-{site}",
                                         png.replace("_over",
                                                     f"_site-{site}_over"))
            if os.path.isfile(file_to_check) is False:
                shutil.copy2(os.path.join(path, png), file_to_check)
                print('copy done')
            liste_site.append(site)

        if pdf:
            for site in set(liste_site):
                images = []
                path_site = os.path.join(path, f"site-{site}")
                for image in os.listdir(path_site):
                    if image.startswith("sub"):
                        png = Image.open(os.path.join(path_site, image))
                        png.load()
                        background = Image.new("RGB",
                                               png.size,
                                               (255, 255, 255))
                        background.paste(png, mask=png.split()[3])
                        images.append(background)

                pdf_path = os.path.join(path,
                                        f"site-{site}",
                                        f"recap_site-{site}.pdf")
                images[0].save(pdf_path, "PDF", resolution=100.0,
                               save_all=True,
                               append_images=images[1:])
                print(pdf_path)


if __name__ == "__main__":
    fire.Fire(cohorte)
