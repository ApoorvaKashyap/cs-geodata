.. cs_geodata documentation master file, created by
   sphinx-quickstart on Mon Jan 12 17:01:33 2026.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

cs_geodata Documentation
=========================

Welcome to **cs_geodata**, a Python library to convert, save, and load geodata from Zarr and Parquet formats.

Introduction
------------

``cs_geodata`` provides handlers for working with geospatial data in various formats. The library focuses on efficient data conversion and storage using modern formats like Zarr for raster data and Parquet for vector data.

Features
~~~~~~~~

* **GeoTIFF Handler**: Load, save, and convert GeoTIFF files to Zarr format
* **GeoJSON Handler**: Process GeoJSON data and convert to Parquet
* **Asynchronous Downloads**: Efficient downloading of geodata with progress tracking
* **Layer Fetching**: Retrieve and categorize geospatial layers from STAC catalogs

Getting Started
---------------

Installation
~~~~~~~~~~~~

Install the package using uv:

.. code-block:: bash

   uv sync --group docs

Quick Example
~~~~~~~~~~~~~

.. code-block:: python

   from src.handlers.geotiff import GeoTiffHandler

   # Load a GeoTIFF file and convert to Zarr
   handler = GeoTiffHandler("path/to/file.tif")
   data = handler.load()
   handler.save(data, "output.zarr", fmt="zarr")

API Reference
-------------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules

Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
