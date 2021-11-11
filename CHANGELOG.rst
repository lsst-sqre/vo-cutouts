##########
Change log
##########

0.1.0 (2021-11-11)
==================

Initial version, which uses a colon-separated tuple as the ``ID`` parameter and has an initial proof-of-concept backend that runs ``pipetask`` via ``subprocess`` and does not do correct conversion of coordinates to pixels.

This is only a proof of concept release.
Some of the things it does are very slow and block the whole asyncio process.
The backend will be changed significantly before the first production release.
