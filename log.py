import docker
import pandas as pd
import time
from datetime import datetime

client = docker.from_env()

def monitor_containers():
    monitored_containers = {}
    while True:
        active_containers = client.containers.list(filters={"name": "robot"})
        active_robot_containers = [c for c in active_containers if c.name.startswith("robot")]

        for container in active_robot_containers:
            if container.id not in monitored_containers:
                monitored_containers[container.id] = {
                    "name": container.name,
                    "stats": [],
                    "prev_cpu_stats": None, # Store previous CPU stats for calculation
                }

        for container_id, info in list(monitored_containers.items()):
            container = client.containers.get(container_id)
            if container.status == 'running':
                stats = container.stats(stream=False)
                cpu_usage = stats["cpu_stats"]["cpu_usage"]["total_usage"]
                if not 'system_cpu_usage' in stats["cpu_stats"]:
                    continue
                system_cpu_usage = stats["cpu_stats"]["system_cpu_usage"]
                num_cpus = len(stats["cpu_stats"]["cpu_usage"]["percpu_usage"])

                # Calculate CPU usage percentage
                if info["prev_cpu_stats"] is not None:
                    cpu_usage_delta = cpu_usage - info["prev_cpu_stats"]["cpu_usage"]
                    system_cpu_usage_delta = system_cpu_usage - info["prev_cpu_stats"]["system_cpu_usage"]
                    cpu_usage_percentage = (cpu_usage_delta / system_cpu_usage_delta) * num_cpus * 100
                else:
                    cpu_usage_percentage = 0 # Initial value

                # Update previous CPU stats for the next calculation
                info["prev_cpu_stats"] = {
                    "cpu_usage": cpu_usage,
                    "system_cpu_usage": system_cpu_usage,
                }

                # Check if 'usage' key exists in memory_stats
                if 'usage' in stats["memory_stats"]:
                    mem_usage = stats["memory_stats"]["usage"]
                else:
                    mem_usage = None # Handle the case where 'usage' key is not present

                info["stats"].append((time.time(), cpu_usage_percentage, mem_usage))
            else:
                write_stats_to_csv(info["name"], info["stats"])
                del monitored_containers[container_id]

        if not monitored_containers:
            break

        time.sleep(2) # Check every 10 seconds

def write_stats_to_csv(container_name, stats):
    df = pd.DataFrame(stats, columns=["Timestamp", "CPU Usage Percentage", "Memory Usage"])
    timestamp = datetime.now().strftime("%y%m%d%H%M")
    filename = f"{container_name}_{timestamp}.csv"
    df.to_csv(filename, index=False)
    print(f"Data for {container_name} written to {filename}")

if __name__ == "__main__":
    monitor_containers()
