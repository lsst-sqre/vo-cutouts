# vo-cutouts

vo-cutouts is a [FastAPI](https://fastapi.tiangolo.com/) web service and associated backend worker implementation that implements an image cutout service for the Rubin Science Platform.
The underlying work of generating the cutout is delegated to [lsst.image_cutout_backend](https://github.com/lsst-dm/image_cutout_backend/).

See [CHANGELOG.md](https://github.com/lsst-sqre/vo-cutouts/blob/main/CHANGELOG.md) for the change history of vo-cutouts.

This is the concrete implementation of the architecture in [DMTN-139](https://dmtn-139.lsst.io/) (not yet published) and the design in [DMTN-208](https://dmtn-208.lsst.io/).

vo-cutouts implements the image cutout portion of the IVOA [SODA](https://ivoa.net/documents/SODA/20170517/REC-SODA-1.0.html) specification.
