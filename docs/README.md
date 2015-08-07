# Scalding Docs

This folder is the source of the [Scalding ReadTheDocs website](http://scalding.readthedocs.com).

Any change to this directory will update the corresponding branch of the website.

The following Github branches correspond to branches on the website:

* master -> http://scalding.readthedocs.com/en/stable
* develop -> http://scalding.readthedocs.com/en/latest
## Build Process
Whenever someone pushes a change to either of the above listed branches, Github alerts ReadTheDocs to rebuild that branch's documentation.

The following events then happen:
* One of ReadTheDocs's machines clones the newest version of the branch.
* The machine runs `pip install -r docs/requirements.txt` to make sure it has the correct python dependencies to build this project.
* The machine runs `python setup.py`.
    * `setup.py` first downloads a version of JDK 7. (This is because ReadTheDocs's machines don't come with a JDK.)
    * `setup.py` then calls `./sbt docs/tut`, which triggers `tut` to compile every file in `docs` that matches the regex `"(.*\\.(md|rst))|conf.py|Makefile|requirements.txt|github.css|custom.css".r` to the corresponding path in `docs-gen`. (For example, `docs/index.rst` will compile to `docs-gen/index.rst`.)

      Note that since the sbt `docs` project depends on `scaldingCore`, `scaldingRepl`, and `scaldingCommons`, these three projects are compiled before tut generates the documentation.
* The machine then runs `sphinx-build` in the `docs-gen` directory, which generates the html files. (I don't know exactly what arguments it gives to `sphinx-build`, because I don't use `sphinx-build` directly on my local machine. See "Building on your Local Machine" below to learn how to build the docs locally.)

## Building on your Local Machine
* Make sure that your machine has the following:
    * python (I use python3, although these scripts might run with python2 as well.)
    * pip (I use pip3, which is pip for Python 3.)
    * The correct pip dependencies. You can do this by running `pip install -r docs/requirements.txt`.

> **Note**: depending on how your pip is set up, it might install the packages globally (i.e. in your home directory or /usr/local/bin or something) or locally (in the current working directory). If it installs to your current directory, it will put packages in `./src`. Either one is fine, but be careful if it installs the packages in your local directory! Make sure not to commit pip packages to scalding source.

* Edit the documentation files in `docs`. If you want to include code that actually gets run in a Scala REPL, format your code like:
    ```
        ```tut
        val x = 5
        val y = x + 2
        ```
    ```
    When you run your code through tut (the next step), it will output it in `docs/gen` like this:
    ```
        ```scala
        scala> val x = 5
        x: Int = 5

        scala> val y = x + 2
        y: Int = 7
        ```
    ```
    For more information about tut, see https://github.com/tpolecat/tut
* Run tut.

  You can do this by opening sbt (`./sbt`) and running the command `docs/tut`. You can also run tut on a single file by running, for example, `docs/tutOnly reference/index.rst`. \s

  Tut will copy every file in `docs` to the corresponding path in `docs-gen`. If a file has a tut block, it will run it in a REPL and put the results in the corresponding `docs-gen` location. If not, tut will make an identical copy of the file in the corresponding `docs-gen` location.

> **Note:** if you want a clean build, (useful if, for example, you renamed/deleted/moved a file in docs/) you should do `rm -rf docs-gen/*` before running tut.

* Run the sphinx build.

  Do this by CDing into `docs`, and running `make clean html`. (Sphinx will convert everything in the `docs/gen` directory to HTML files in the `docs/_build/html` directory.)
* Preview the site!

  I do this by Cding into `docs/_build` and running `python3 -m http.server`.

> **Note:** Nothing that you build locally on your machine (i.e. stuff in the `docs/gen` and `docs/_build` directories) should ever end up in source control. They've been gitignore'd.
