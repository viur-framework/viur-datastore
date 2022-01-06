The Datastore wrapper for ViUR-Core
-----------------------------------

This repository contains the datastore wrapper for the ViUR framework.
We build our own wrapper around it's REST-API as the original wrapper has significant CPU overhead.
Using a fast JSON-Parser (simdjson) and Cython, we can go directly from the received JSON-encoded protocol-wrapper
representation to the final python objects, without converting that JSON into an intermediate python object
representation which then gets discarded right away.

While it has some ViUR specific functions, it's also possible to use this wrapper outside ViUR.
