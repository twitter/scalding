# Scald.rb

The `scald.rb` script in the `scripts/` directory is a handy script that makes it easy to run jobs in both local mode or on a remote Hadoop cluster. It handles simple command-line parsing, and copies over necessary JAR files when running remote jobs.

If you're running many Scalding jobs, it can be useful to add `scald.rb` to your path, so that you don't need to provide the absolute pathname every time. One way of doing this is via (something like):

```bash
ln -s scripts/scald.rb $HOME/bin/
```

This creates a symlink to the `scald.rb` script in your `$HOME/bin/` directory (which should already be included in your PATH).

More information coming soon.
