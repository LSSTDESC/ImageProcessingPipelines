# Pipeline setup files

## Main tasks file

The main tasks file has been separated into 2 files, one to run at
NERS, the other to run at CC-IN2P3. A `diff` between these two file
should show as little difference as possible, i.e.,

       11c11
       <       <var name="SITE">LSST-IN2P3</var>
       ---
       >       <var name="SITE">NERSC</var>