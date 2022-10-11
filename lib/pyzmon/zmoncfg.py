import pprint
import os
import json
from pyzmon.slog import (slog, sfatal)

_cfg_inst = None

def getcfg(script_pathname=None, cfg=None):
    global _cfg_inst
    if not _cfg_inst:
        if script_pathname:
            _cfg_inst = ZmonCfg(script_pathname=script_pathname, cfg=cfg)
        else:
            sfatal("No script_pathname provided during first getcfg invocation")

    return _cfg_inst

class ZmonCfg:
    def __init__(self, script_pathname=None, cfg=None):
        self.script_pathname = script_pathname
        self._dirname, self.scriptname = os.path.split(script_pathname)
        self._zmon_basedir = os.path.dirname(self._dirname)
        self._cfg_file = os.path.join(self._zmon_basedir, 'etc', self.scriptname + '.json')

        self.cfg = {
            }    

        self.mk_cfg(cfg)
        #print("config file: " + self._cfg_file)
        

    def mk_cfg(self, cust_cfg):
        if cust_cfg:
            self.update_cfg(cust_cfg)

        if os.path.exists(self._cfg_file):
            with open(self._cfg_file, 'r') as f:
                jcfg = json.load(f)
                self.update_cfg(jcfg)
                
    def update_cfg(self, newcfg):
        for k in newcfg:
            if not k.startswith('_'):
                self.cfg[k] = newcfg[k]

if __name__ == '__main__':
    scriptcfg = getcfg(script_pathname=__file__)
    pprint.pprint(scriptcfg.cfg)
