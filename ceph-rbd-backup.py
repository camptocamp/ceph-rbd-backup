#!/usr/bin/env python

import json
import subprocess
import datetime
import logging
import sys


## stamp_day_after:
# False = last snapshot and replicator run on same date
# True = snapshots before midnight, replicator after midnight
#stamp_day_after = False
stamp_day_after = True

## snapshot_mounted_only:
# If True, only make snapshots of mounted volumes, otherwise
# do it for all mapped rbd images
snapshot_mounted_only = True

## check_threshold_time
# Time of day after which the 'check' action will start reporting
# missing replications and snapshots
check_threshold_time = datetime.time(hour=8)

## Ceph credentials
ceph_conf_prod = '/etc/ceph/ceph.conf'
ceph_conf_backup = '/etc/ceph/ceph-backup.conf'
ceph_keyring_prod = '/etc/ceph/ceph.client.rbd.keyring'
ceph_keyring_backup = '/etc/ceph/ceph.client.rbd_backup.keyring'
ceph_username_prod = 'rbd'
ceph_username_backup = 'rbd_backup'



class Stamp:
  def __init__(self):
    self.delta_days = stamp_day_after and 1 or 0

  def today(self):
    """ Last expected snapshot name at replication time """
    return self._stamp(datetime.date.today() - datetime.timedelta(hours=self.delta_days*24))

  def yesterday(self):
    """ Before last expected snapshot name at replication time """
    return self._stamp(datetime.date.today() - datetime.timedelta(hours=(self.delta_days+1)*24))

  def now(self):
    """ Snapshot name to use at snapshot creation time """
    return self._stamp(datetime.date.today())

  def _stamp(self, date):
    return date.strftime("%Y-%m-%d")



class Rbd:
  def __init__(self, config, keyring, username):
    self.config = config
    self.keyring = keyring
    self.username = username

  def _rbd_base_cmd(self, json=True):
    return ['rbd', '-c', self.config, '--keyring', self.keyring, '--id', self.username] + (json and ['--format', 'json'] or [])

  def _rbd_exec_simple(self, *args):
    rbd_cmd = self._rbd_base_cmd() + list(args)
    logging.debug("_rbd_exec_simple cmd: " + repr(rbd_cmd))
    rbd_sp = subprocess.Popen(rbd_cmd, stdout=subprocess.PIPE)
    out, err = rbd_sp.communicate()
    logging.debug("_rbd_exec_simple stdout: " + out)
    result = json.loads(out)
    return result

  def _rbd_exec_noout(self, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_noout cmd: " + repr(rbd_cmd))
    rbd_sp = subprocess.Popen(rbd_cmd)
    out, err = rbd_sp.communicate()

  def _rbd_exec_pipe_source(self, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_pipe_source cmd: " + repr(rbd_cmd))
    rbd_sp = subprocess.Popen(rbd_cmd, stdout=subprocess.PIPE)
    return rbd_sp.stdout

  def _rbd_exec_pipe_dest(self, source_pipe, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_pipe_dest cmd: " + repr(rbd_cmd))
    rbd_sp = subprocess.Popen(rbd_cmd, stdin=source_pipe, stdout=subprocess.PIPE)
    source_pipe.close() # ?
    out, err = rbd_sp.communicate()
    logging.debug(out)

  def list(self):
    return self._rbd_exec_simple('list')

  def create(self, image_name, size):
    self._rbd_exec_noout('create', image_name, '--size', str(size))

  def snap_list(self, image_name):
    return self._rbd_exec_simple('snap', 'list', image_name)

  def snap_list_names(self, image_name):
    return [snap['name'] for snap in self.snap_list(image_name)]

  def snap_create(self, image_name, snap_name):
    self._rbd_exec_noout('snap', 'create', image_name, '--snap', snap_name)

  def export_diff(self, image_name, snap_name, from_snap_name=None):
    from_snap_args = from_snap_name and ['--from_snap', from_snap_name] or []
    return self._rbd_exec_pipe_source('export-diff', image_name, '-', '--snap', snap_name, *from_snap_args)

  def import_diff(self, image_name, source_pipe):
    return self._rbd_exec_pipe_dest(source_pipe, 'import-diff', '-', image_name)

  def showmapped(self):
    return self._rbd_exec_simple('showmapped').values()
      


class ReplicatorImageException(Exception):
  pass


class Replicator:
  def __init__(self, src_rbd, dst_rbd):
    self.src_rbd = src_rbd
    self.dst_rbd = dst_rbd

  def _replicate_snap_full(self, image):
    logging.info("Doing full replication for image '%s'" %(image))
    self.dst_rbd.import_diff(image, self.src_rbd.export_diff(image, Stamp().today()) )
    
  def _replicate_snap_diff(self, image):
    logging.info("Doing diff replication for image '%s'" %(image))
    if Stamp().yesterday() not in self.src_rbd.snap_list_names(image):
      raise ReplicatorImageException("Source image '%s' doesn't have yesterday's snapshot" %(image))
    if Stamp().yesterday() not in self.dst_rbd.snap_list_names(image):
      raise ReplicatorImageException("Destination image '%s' doesn't have yesterday's snapshot" %(image))
    self.dst_rbd.import_diff(image, self.src_rbd.export_diff(image, Stamp().today(), Stamp().yesterday()) )

  def replicate(self, single_image=None):
    logging.info("Starting replication of images to destination")
    errors = False
    for image in self.src_rbd.list():
      if single_image and image != single_image: continue
      logging.info("Replicating image '%s'" %(image))
      try:
        # Check existing snapshot on source (must exist)
        if Stamp().today() not in self.src_rbd.snap_list_names(image):
          raise ReplicatorImageException("Source image '%s' doesn't have today's snapshot %s" %(image, Stamp().today()))
        # If image doesn't exist on destination, create it
        if image not in self.dst_rbd.list():
          logging.info("Creating new image '%s' on destination" %(image))
          self.dst_rbd.create(image, 1)
          if image not in self.dst_rbd.list():
            raise ReplicatorImageException("Error creating new image '%s' on destination" %(image))
        # Check existing snapshot on destination (must not exist)
        if Stamp().today() in self.dst_rbd.snap_list_names(image):
          raise ReplicatorImageException("Destination image '%s' already has today's snapshot" %(image))
        # If destination has no snapshot, do full replication
        if not self.dst_rbd.snap_list_names(image):
          self._replicate_snap_full(image)
        else:
          self._replicate_snap_diff(image)
      except ReplicatorImageException, e:
        errors = True
        logging.error(e.message + " - continuing with next image")
        continue
    logging.info("Finished replication of images to destination" + (errors and " (with errors)" or ""))

  def check(self, threshold_time, single_image=None):
    errors = []
    for image in self.src_rbd.list():
      if single_image and image != single_image: continue
      if datetime.datetime.now().time() < threshold_time:
        # Before threshold time, check for yesterdays (before latest) images on prod and backup clusters
        if Stamp().yesterday() not in self.src_rbd.snap_list_names(image):
          errors.append("%s: missing snapshot %s on production cluster" %(image, Stamp().yesterday()) )
        elif Stamp().yesterday() not in self.dst_rbd.snap_list_names(image):
          errors.append("%s: missing snapshot %s on backup cluster" %(image, Stamp().yesterday()) )
      else:
        # After threshold time, check for image and todays (last) images on prod and backup clusters
        if image not in self.dst_rbd.list():
          errors.append("%s: image missing on backup cluster" %(image))
        elif Stamp().today() not in self.src_rbd.snap_list_names(image):
          errors.append("%s: missing snapshot %s on production cluster" %(image, Stamp().today()) )
        elif Stamp().today() not in self.dst_rbd.snap_list_names(image):
          errors.append("%s: missing snapshot %s on backup cluster" %(image, Stamp().today()) )
    if not errors:
      descr = single_image and "Backup for image %s"%(single_image) or "All backups"
      print "BACKUP OK - %s OK" %(descr)
      sys.exit(0)
    elif len(errors) == 1:
      print "BACKUP ERROR - %s" %(errors[0])
      sys.exit(2)
    else:
      print "BACKUP ERROR - %d errors\n%s" %(len(errors), "\n".join(errors))
      sys.exit(2)


class VolumeException(Exception):
  pass

class Volume:
  def __init__(self, image, device):
    self.image = image
    self.device = device
    self.mountpoint = self._get_mountpoint()
    self.frozen = False

  def __del__(self):
    # ensure we don't leave frozen filesystems behind
    if self.frozen:
      sp_unfreeze = subprocess.Popen(['fsfreeze', '--unfreeze', self.mountpoint])
      sp_unfreeze.communicate()

  def _vol_exec_raw(self, *args):
    logging.debug("_vol_exec_raw cmd: " + repr(args))
    sp_vol = subprocess.Popen(args, stdout=subprocess.PIPE)
    out, err = sp_vol.communicate()
    logging.debug("_vol_exec_raw out: " + out)
    return out

  def _get_mountpoint(self, first_only=True):
    ret = self._vol_exec_raw('findmnt', '-o', 'TARGET', '-n', self.device)
    mpts = ret.strip().split("\n")
    if first_only:
      return mpts and mpts[0] or None
    else:
      return mpts

  def mounted(self):
    return self.mountpoint is not None

  def freeze(self):
    if not self.mounted():
      raise VolumeException("Cannot freeze not mounted volume '%s'" %(self.device))
    self.frozen = True
    self._vol_exec_raw('fsfreeze', '--freeze', self.mountpoint)

  def unfreeze(self):
    if not self.mounted():
      raise VolumeException("Cannot unfreeze not mounted volume '%s'" %(self.device))
    self._vol_exec_raw('fsfreeze', '--unfreeze', self.mountpoint)
    self.frozen = False
 


if __name__=="__main__":

  import argparse
  parser = argparse.ArgumentParser(description='Ceph backup / replication tool')
  parser.add_argument('action', help='action to perform', choices=['replicate', 'snapshot', 'check'])
  parser.add_argument('--image', help='single image to process instead of all')
  parser.add_argument('--debug', help='enable debug logging', action='store_true')
  args = parser.parse_args()

  level = args.debug and logging.DEBUG or logging.INFO
  logging.basicConfig(format='%(levelname)s %(message)s', level=level)

  if args.action == "snapshot":
    ceph_prod = Rbd(ceph_conf_prod, ceph_keyring_prod, ceph_username_prod)
    volumes = [Volume(m['name'], m['device']) for m in ceph_prod.showmapped()]

    logging.info("Starting snapshot of all mounted volumes")
    errors = False
    for volume in volumes:
      if args.image and volume.image != args.image: continue
      if not volume.mounted() and snapshot_mounted_only: continue
      logging.info("Creating snapshot for volume '%s'" %(volume.image))
      if Stamp().now() in ceph_prod.snap_list_names(volume.image):
        logging.error("Image '%s' already has snapshot '%s' - continuing with next image" %(volume.image, Stamp().now()))
        continue
      try:
        if volume.mounted():
          volume.freeze()
        ceph_prod.snap_create(volume.image, Stamp().now())
        if volume.mounted():
          volume.unfreeze()
      except VolumeException, e:
        logging.error(e.message + " - continuing with next volume")
        continue
    logging.info("Finished snapshot of all mounted volumes" + (errors and " (with errors)" or ""))
    
  elif args.action == "replicate":
    ceph_prod = Rbd(ceph_conf_prod, ceph_keyring_prod, ceph_username_prod)
    ceph_backup = Rbd(ceph_conf_backup, ceph_keyring_backup, ceph_username_backup)
    
    replicator = Replicator(ceph_prod, ceph_backup)
    replicator.replicate(single_image=args.image)

  elif args.action == "check":
    ceph_prod = Rbd(ceph_conf_prod, ceph_keyring_prod, ceph_username_prod)
    ceph_backup = Rbd(ceph_conf_backup, ceph_keyring_backup, ceph_username_backup)
    
    replicator = Replicator(ceph_prod, ceph_backup)
    replicator.check(check_threshold_time, single_image=args.image)

