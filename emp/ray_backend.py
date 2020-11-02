__all__ = ['RayService', 'init_ray_backend', 'shutdown_ray_backend']


import re
import os
import ray
import time
import subprocess


ray_service = None


class RayService:

    def __init__(self, head_mode='per_node', wait_seconds=5, port=16379, dashboard_port=18265, verbose=True):
        if 'SLURM_PROCID' in os.environ:
            proc_id = int(os.environ.get('SLURM_PROCID'))
            node_id = int(os.environ.get('SLURM_NODEID'))
            local_id = int(os.environ.get('SLURM_LOCALID'))
            num_nodes = int(os.environ.get('SLURM_NNODES'))
            ntasks_per_node = int(os.environ.get('SLURM_NTASKS_PER_NODE', '1'))

            nodelist = os.environ['SLURM_NODELIST']
            head_ip = re.findall('.*-(\d+)-(\d+)-(\d+)-\[?(\d+)', nodelist)[0]  # noqa
            assert len(head_ip) == 4, repr(head_ip)
            head_ip = '.'.join(head_ip)

            nodename = os.environ['SLURMD_NODENAME']
            curr_ip = re.findall('.*-(\d+)-(\d+)-(\d+)-\[?(\d+)', nodename)[0]  # noqa
            assert len(curr_ip) == 4, repr(curr_ip)
            curr_ip = '.'.join(curr_ip)

            cmd_prefix = f'ray start --block --dashboard-host 0.0.0.0'
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'

            if head_mode == 'per_job':
                if proc_id == 0:
                    cmd = f'{cmd_prefix} --dashboard-port {dashboard_port} --port {port} --head'
                    if verbose:
                        print(f'launch ray service on rank {proc_id}: [{cmd}]', flush=True)
                    self.server_process = subprocess.Popen(cmd, shell=True, env=env)
                time.sleep(wait_seconds)

                if proc_id != 0 and proc_id == node_id * ntasks_per_node:
                    cmd = f'{cmd_prefix} --dashboard-port {dashboard_port} --address {head_ip}:{port}'
                    if verbose:
                        print(f'launch ray service on rank {proc_id}: [{cmd}]', flush=True)
                    self.server_process = subprocess.Popen(cmd, shell=True, env=env)
                if num_nodes > 1:
                    time.sleep(wait_seconds)

                ray.init(address=f'{head_ip}:{port}')
            elif head_mode == 'per_node':
                if proc_id == node_id * ntasks_per_node:
                    cmd = f'{cmd_prefix} --dashboard-port {dashboard_port} --port {port} --head'
                    if verbose:
                        print(f'launch ray service on rank {proc_id}: [{cmd}]', flush=True)
                    self.server_process = subprocess.Popen(cmd, shell=True, env=env)
                time.sleep(wait_seconds)

                ray.init(address=f'{curr_ip}:{port}')
            elif head_mode == 'per_rank':
                dashboard_port += local_id
                port += local_id
                cmd = f'{cmd_prefix} --dashboard-port {dashboard_port} --port {port} --head'
                if verbose:
                    print(f'launch ray service on rank {proc_id}: [{cmd}]', flush=True)
                self.server_process = subprocess.Popen(cmd, shell=True, env=env)
                time.sleep(wait_seconds)

                ray.init(address=f'{curr_ip}:{port}')
            else:
                raise ValueError(f'unknown head_mode: {head_mode}')
        else:
            ray.init(dashboard_host='0.0.0.0', dashboard_port=dashboard_port)

        self.verbose = verbose

    def shutdown(self):
        ray.shutdown()
        if hasattr(self, 'server_process'):
            self.server_process.terminate()
            self.server_process.wait()
            del self.server_process
        if self.verbose:
            print('shutdown ray service', flush=True)


def init_ray_backend(head_mode='per_node', wait_seconds=10, port=16379, dashboard_port=18265, verbose=True):
    global ray_service
    if ray_service is None:
        ray_service = RayService(head_mode, wait_seconds, port, dashboard_port, verbose)


def shutdown_ray_backend():
    global ray_service
    if ray_service is not None:
        ray_service.shutdown()
        ray_service = None
