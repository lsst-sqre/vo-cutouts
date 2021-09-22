##########
vo-cutouts
##########

|Build|

vo-cutouts is a `FastAPI`_ web service and associated backend worker implementation that implements an image cutout service for the Rubin Science Platform.
The underlying work of generating the cutout is delegated to a pipelines task.

.. _FastAPI: https://fastapi.tiangolo.com/

This is the concrete implementation of the architecture in `DMTN-139`_ (not yet published) and the design in `DMTN-208`_ (not yet published).

.. _DMTN-139: https://dmtn-139.lsst.io/
.. _DMTN-208: https://dmtn-208.lsst.io/

vo-cutouts implements the image cutout portion of the IVOA `SODA`_ specification.

.. _SODA: https://ivoa.net/documents/SODA/20170517/REC-SODA-1.0.html

.. |Build| image:: https://github.com/lsst-sqre/vo-cutouts/workflows/CI/badge.svg
   :alt: GitHub Actions
   :scale: 100%
   :target: https://github.com/lsst-sqre/vo-cutouts/actions
