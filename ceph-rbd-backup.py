#!/usr/bin/env python

import json
import subprocess
import datetime
import logging
import sys
import os


## Ceph credentials
ceph_conf_prod = '/etc/ceph/ceph.conf'
ceph_conf_backup = '/etc/ceph/ceph-backup.conf'
ceph_keyring_prod = '/etc/ceph/ceph.client.rbd.keyring'
ceph_keyring_backup = '/etc/ceph/ceph.client.rbd_backup.keyring'
ceph_username_prod = 'rbd'
ceph_username_backup = 'rbd_backup'

replication_lockfile_pattern = '/tmp/ceph-rbd-back-%s.lock'

class Rbd:
  def __init__(self, config, keyring, username, noop=False):
    self.config = config
    self.keyring = keyring
    self.username = username
    self.noop = noop

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
    if self.noop:
      logging.info("_rbd_exec_noout noop! (%s)" %(' '.join(rbd_cmd)))
    rbd_sp = subprocess.Popen(rbd_cmd)
    out, err = rbd_sp.communicate()

  def _rbd_exec_pipe_source(self, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_pipe_source cmd: " + repr(rbd_cmd))
    if self.noop:
      logging.info("_rbd_exec_pipe_source noop! (%s)" %(' '.join(rbd_cmd)))
      return None
    rbd_sp = subprocess.Popen(rbd_cmd, stdout=subprocess.PIPE)
    return rbd_sp.stdout

  def _rbd_exec_pipe_dest(self, source_pipe, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_pipe_dest cmd: " + repr(rbd_cmd))
    if self.noop:
      logging.info("_rbd_exec_pipe_dest noop! (%s)" %(' '.join(rbd_cmd)))
      return None
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
  parser.add_argument('--noop', help='don\'t do any action, only log', action='store_true')
  args = parser.parse_args()

  level = args.debug and logging.DEBUG or logging.INFO
  logging.basicConfig(format='%(levelname)s %(message)s', level=level)

  ceph_prod = Rbd(ceph_conf_prod, ceph_keyring_prod, ceph_username_prod, args.noop)
  ceph_backup = Rbd(ceph_conf_backup, ceph_keyring_backup, ceph_username_backup, args.noop)

  if args.action == "snapshot":
    volumes = [Volume(m['name'], m['device']) for m in ceph_prod.showmapped()]
    logging.info("Starting snapshot of all mounted volumes")
    errors = False
    for volume in volumes:
      if args.image and volume.image != args.image: continue
      if not volume.mounted() and snapshot_mounted_only: continue
      logging.info("Creating snapshot for volume '%s'" %(volume.image))
      today = datetime.date.today().strftime("%Y-%m-%d")
      if today in ceph_prod.snap_list_names(volume.image):
        logging.error("Image '%s' already has snapshot '%s' - continuing with next image" %(volume.image, today))
        continue
      try:
        if volume.mounted():
          volume.freeze()
        ceph_prod.snap_create(volume.image, today)
        if volume.mounted():
          volume.unfreeze()
      except VolumeException, e:
        logging.error(e.message + " - continuing with next volume")
        continue
    logging.info("Finished snapshot of all mounted volumes" + (errors and " (with errors)" or ""))

  elif args.action == "replicate":
    logging.info("Starting replication of images to destination")
    for image in ceph_prod.list():
      if args.image and image != args.image: continue
      logging.info("Replicating image '%s'" %(image))
      if os.path.exists(replication_lockfile_pattern %(image)):
        logging.info("Lock file found, skipping replication")
        continue
      open(replication_lockfile_pattern %(image),'w')
      if image not in ceph_backup.list():
        logging.info("Creating new image '%s' on destination" %(image))
        ceph_backup.create(image, 1)
      latest_bk_snap = ceph_backup.snap_list_names(image)[-1]
      latest_prd_snap = ceph_prod.snap_list_names(image)[-1]
      if latest_bk_snap == latest_prd_snap:
        logging.error("Latest snapshot '%s' for image '%s' already present on backup - skipping" %(latest_prd_snap, image))
        os.unlink(replication_lockfile_pattern %(image))
        continue
      elif latest_bk_snap not in ceph_prod.snap_list_names(image):
        logging.info("Latest backup snapshot '%s' for image '%s' missing on prod, doing full replication" %(latest_bk_snap, image))
        ceph_backup.import_diff(image, ceph_prod.export_diff(image, latest_prd_snap) )
      else:
        logging.info("Doing diff replication for image '%s'" %(image))
        ceph_backup.import_diff(image, ceph_prod.export_diff(image, latest_prd_snap, latest_bk_snap) )
      os.unlink(replication_lockfile_pattern %(image))

  elif args.action == "check":
    errors = []
    for image in ceph_prod.list():
      if args.image and image != args.image: continue
      latest_bk_snap = ceph_backup.snap_list_names(image)[-1]
      latest_prd_snaps = ceph_prod.snap_list_names(image)[-2:]
      if image not in ceph_backup.list():
        errors.append("%s: missing image on backup cluster" %(image))
      elif latest_bk_snap not in latest_prd_snaps:
        errors.append("%s: latest backup snapshot %s not up-to-date with production %s" %(image, latest_bk_snap, latest_prd_snaps[-1]))
    if not errors:
      descr = args.image and "Backup for image %s"%(args.image) or "All backups"
      print "BACKUP OK - %s OK" %(descr)
      sys.exit(0)
    elif len(errors) == 1:
      print "BACKUP ERROR - %s" %(errors[0])
      sys.exit(2)
    else:
      print "BACKUP ERROR - %d errors\n%s" %(len(errors), "\n".join(errors))
      sys.exit(2)

