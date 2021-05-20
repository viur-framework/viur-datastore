An *experimental* datastore accelerator for ViUR
------------------------------------------------

This repository contains a hackish Cython wrapper around the datastore REST-Api method lookup.
The current google.cloud.datastore implementation has some serious performance penalties when retrieving
many or larger entities. This slim wrapper can be used instead for a significant speed gain.
We utilize the simdjson to convert the received json-encoded protocol buffer directly to the Entity/Key classes
provided by google.cloud.datastore - without converting that json to an intermediate set of python dicts/lists,
gaining a vast reduction in CPU cycles.  

> :warning: **This is a very early experimental proof-of-concept.** 

