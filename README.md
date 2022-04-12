# The Datastore wrapper for ViUR-Core

This repository contains the datastore wrapper for the ViUR framework.
We build our own wrapper around it's REST-API as the original wrapper has significant CPU overhead.
Using a fast JSON-Parser (simdjson) and Cython, we can go directly from the received JSON-encoded protocol-wrapper
representation to the final python objects, without converting that JSON into an intermediate python object
representation which then gets discarded right away.

While it has some ViUR specific functions, it's also possible to use this wrapper outside ViUR.

## Using ##

Just install it via venv, pipenv or requirements.txt into your appengine project. As by now it's a drop in replacement
for the original library provided by google, but maybe we break api compatibility in a future
release for more performance.

## Developing

As a developer we want to have an editable viur-datastore library. We're going to use pipenv for the job
and install needed 

    cd <path/to/viur-testing-project>
    pipenv install
    pipenv shell
    pipenv install build twine
    pipenv install -e <path/to/viur-datastore>

Choose a testing project configuration via gcloud if you use configs per projectIds:

    gcloud config configurations activate <project-config>

Or use environment variable which projectId we want to use:

    export CLOUDSDK_CORE_PROJECT="my-awesome-testing-project"

Yezzzz - now we build that awesome new stuff!:

    python -m build

Please take a cup of tea - oh wait - it has just build in seconds.
If everything worked fine, you should see some builds in the dist subfolder.

## Testing ##

Now it's time run our test suite before actually releasing our shiny new version:
       
    cd <path/to/viur-datastore>
    python -m unittest tests

If everything worked fine and all tests passed you can go on with the release procedure.

## Releasing ##

After building **and** testing the new version please update changelog, commit everything and tag it with the
same version you used in setup.py.

Do you have already installed twine? No? Ok, go back to the developing step!!!

Now please manually bump the version in setup.py, safe the file and here we go.

We only want to provide and upload the generic build - perhaps we'll provide optimized
builds per python version, OS and arch in a future release.

Now it's time to get an api key **exclusively** for this project from pypi and save it to your wallet!

Now the actual release workflow:

    cd <path/to/viur-datastore>
    twine upload dist/viur-datastore-<version>.tar.gz

You might already have a keyring configured with credentials /api key for pypi or
provide username `__token__` and password `<your api key>` in interactive mode.
