# Sentinel-1 Inventory Checking

Basic process is to parse the Sentinel-1 inventory and find those paths
that have less than 8 files in them.

We are then going to undelete them from the staging bucket and copy
them in place, so that they can be replicated.

To do this, run the scripts in order (with an AWS environment set up):

* `save_inventory.py` - drops all the inventory contents to a zipped text file
* `dump_missing.py` - parses this zipped text file and finds those that have < 8
  files and dumps those.
* `fix_all.py` deletes delete markers and copies files in place.
