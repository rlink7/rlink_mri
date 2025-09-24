## Usage

![PythonVersion](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9-blue)
![License](https://img.shields.io/badge/License-CeCILLB-blue.svg)
![PoweredBy](https://img.shields.io/badge/Powered%20by-CEA%2FNeuroSpin-blue.svg)

## Development

![Pep8](https://github.com/rlink7/rlink_mri/actions/workflows/pep8.yml/badge.svg)


# R-Link MRI processings

The following processings are available in the derivatives folder.

##### Table of Contents  

[Defacing](#deface)
[Quasi-Raw](#quasiraw)
[CAT12VBM](#cat12vbm)
[FreeSurfer](#freesurfer)
[dMRIprep](#dmriprep)
[TBSS](#tbss)
[Li2MNI](#li2mni)

## deface

Deface structural T1w images.

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> The UK-Biobank study uses a customized image processing pipeline based on FSL (<b>Alfaro-Almagro et al., 2018</b>) which includes a defacing approach. It was designed for use with T1w images. This defacing approach was later extracted from the larger processing pipeline and released as part of the main FSL package as <i>fsl_deface</i>. Like other tools, such as <i>mri_deface</i> and <i>pydeface</i>, this method uses linear registration to locate its own pre-defined mask of face voxels on the target image, then sets voxels in the mask to zero. Unlike <i>mri_deface</i> and pydeface, this method also removes the ears. We used <i>fsl_deface</i> as included in FSL (<b>Jenkinson and Smith (2001)</b>) with default settings.<br>
    <b>Quality control:</b> A manual quality control was performed.
</p>
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>

## quasiraw

Simple affine (no shearing) registration to the MNI template space from
T1w MRIs.

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> Minimally preprocessed data is generated using ANTS (<b>Avants et al. (2009)</b>) bias field correction, FSL FLIRT (<b>Jenkinson and Smith (2001)</b>) with a 9 degrees of freedom (no shearing) affine transformation to register data to the MNI template, and the application of a brain mask to remove non-brain tissues in the final images.<br>
     <b>Quality control:</b> First, we compute the correlation between each image and the mean of every other images to sort them by increasing correlation score. Then, images are manually inspected in-house following this sorting, and a first threshold is set to remove the first outlier images. Additionally, we use the average correlation (using Fisher's z-transform) between registered images as a metric of quality and we retained only images at a threshold higher than 0.5.
</p>
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>

## cat12vbm

Voxel-wise estimation of the local amount or volume of a specific tissue
compartment from T1w MRIs. Use the longitudinal option to refine results.

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> Voxel-Based Morphometry (VBM) is performed with CAT12 (<b>Gaser and Dahnke (2016)</b>). The analysis stream includes non-linear spatial registration to the MNI template, Gray Matter (GM), White Matter (WM), and CerebroSpinal Fluid (CSF) tissues segmentation, bias correction of intensity non-uniformities, and segmentations modulation by scaling with the amount of volume changes due to spatial registration. VBM is applied to investigate the GM, and the longitudinal model allows the detection of small changes, such as brain plasticity or treatement effects after a few weeks or months. The sensitivity of VBM in the WM is low, and usually, diffusion-weighted imaging is preferred for that purpose. For this reason, only the modulated GM images is considered. Moreover, CAT12 computes GM volumes averaged on the Neuromorphometrics atlas that includes 284 brain cortical and sub-cortical ROIs. <br>
      <b>Quality control:</b> We performe the same in-house QC visual analysis as for quasi-raw images. Additionally, we also monitored the Noise Contrast Ratio (NCR) and Image Quality Rating (IQR) as two metrics of quality and we retained only images at a threshold below 4.
</p>
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>

## freesurfer

Parcellisation and segmentation of structural T1w MRI data with FreeSurfer.
Use the longitudinal option to refine results.

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> Cortical analysis is performed with FreeSurfer *recon-all*. The analysis stream includes intensity normalization, skull stripping, segmentation of GM (pial) and WM, hemispheric-based tessellations, topology corrections and inflation, and registration to the *fsaverag* template. Available morphological measures are summarized on the Desikan (<b>Desikan et al. (2006)</b>) and Destrieux (<b>Fischl et al. (2004)</b>) parcellations. Specifically, 7 ROI-based features computed both on Desikan and Destrieux atlases are shared including: the cortical thickness (mean and standard deviation), GM volume, surface area, integrated mean and Gaussian curvatures and intrinsic curvature index. Moreover, vertex-wise cortical thickness, curvature and average convexity features (<b>Fischl et al. (1999)</b>) (measuring the depth/height of a vertex above the average surface) are also accessible on the high-resolution seven order icosahedron. To allow inter-hemispheric cortical surface-based analysis, we further transform the right hemisphere features into the left one, using the symmetric *fsavarage_sym* Freesurfer template and the *xhemi* routines (<b>Greve et al. (2013)</b>). The final vertex-wise cortical features comprise 163,842 nodes per hemisphere.<br>
      <b>Quality control:</b> Similarly with quasi-raw and VBM, we first performe a visual analysis on images ranked by the correlation score. In addition we use the Euler number as a metric of quality and we retaine images at a threshold greater than -217, as specified in (<b>Rosen et al. (2018)</b>).
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>

  
## dmriprep

Preprocessing of DWI using topup susceptinility induced distortions
corrections. Description adapted from
https://doi.org/10.1016/j.schres.2025.08.010.

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> The diffusion data were preprocessed and quality-checked with the following pipeline built around MRTrix3 (Tournier et al. 2019), FSL (Jenkinson et al. 2012), and ANTs (Tustinson 2014) software packages. First, any volumes with a corresponding b value less than 50 were treated as b0 volume for the remainder of the pipeline. The diffusion data were denoised with the provided dwidenoise (MP-PCA) function included with MRTrix3 (Veraart et al. 2016, Cordero-Grande et al. 2019). The images were then intensity-normalized to the first image and concatenated for further processing. FSL's topup and eddy algorithms were used to correct for susceptibilty-induced and motion artifacts and eddy currents and to remove outlier slices (Andersson, et al. 2003, Smith, et al.2004, Andersson, et al. 2016). Lastly, the preprocessed data were fitted with a tensor model using the
dwi2tensor function included with MRTrix3 using an iterative reweighted least squares estimator (Veraart et al. 2013).<br>
    <b>Quality control:</b>  The quality of this preprocessing pipeline was then assessed qualitatively for gross errors and quantitatively analyzed using a three-step approach. In the first step, the preprocessed data were analyzed in accordance with the method outlined by Lauzon (Lauzon et al. 2013). The brain parenchyma without CSF were masked in a restrictive manner by using an eroded brain mask generated on the average b0 image using the bet2 function included with FSL (Smith et al. 2002). Then, the tensor fits of the masked data were backpropagated through the diffusion model to reconstruct the original diffusion signal. The goodness-of-fit for the tensor model was then assessed using a modified pixel chi-squared value per slice per volume. In the second step, the tensor fit was converted to a fractional anisotropy (FA) image. The ICBM FA MNI atlas with 48 white matter tract labels provided with FSL were then non-rigidly registered to the subject's FA image with the ANTs software package (Mori, et al. 2005, Wakana et al. 2007, Hua et al. 2008, Avants et al. 2008). The average FA for each tract was then quantified and assessed for physiologic congruence. Lastly, the gradient orientations were visualized and checked using the dwigradcheck script included with MRTrix (Jeurissen, 2014). Then we automate the process by checking for the FA value of five specific bundles:
    
    - Genu_of_corpus_callosum_med 
    - Body_of_corpus_callosum_med
    - Splenium_of_corpus_callosum_med
    - Corticospinal_tract_L_med
    - Corticospinal_tract_R_med
    
Mean FA values must range in [0.3, 0.75].
</p>
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>

## tbss

Voxel-wise statistics on the skeletonized FA data using FA, MD, ...

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> Once DWI data have been pre-processed, the FA images are nonlinearly registered to the ENIGMA template. All subjects' FA and MD data are then projected onto the ENIGMA FA skeleton.<br>
    <b>Quality control:</b> We manually checked the FA images that have been registered.
</p>
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>

## li2mni

Registration of the Lithium image to the MNI template space using
intermediate Lithium and Hydrogen T1w images.

<details>
  <summary>Paper description (click me)</summary>
<p>
    <br>
    <b>Steps:</b> Lithium and Hydrogen T1w images are bias field corrected with FSL (<b>Jenkinson and Smith (2001)</b>). Then registration steps are performed with ANTS (<b>Avants et al. (2009)</b>). An affine transformation with 9 degrees of freedom (no shearing) coregisters the Lithium T1w and the Hydrogen T1w images, and an affine/non-linear deformation maps the Hydrogene T1w image to the 2mm isotropic MNI space. We assume that the Lithium and Lithium T1w images have the same field of view. Finally, we use the phantom data to calibrate the lithium image intensities by applying a global scaling.<br>
      <b>Quality control:</b> coming soon.
</p>
<p align='right'>
    <b>- NeuroSpin support team</b> <i>(Ways to Simplify Your Writing)</i>
</p>
</details>
