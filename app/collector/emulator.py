import random
import json
from datetime import datetime
from typing import List, Dict

# Эмулятор Windows Event Log для демонстрации
class WindowsLogEmulator:

    # Шаблоны сообщений
    MESSAGES = [
        "User '{}' logged in successfully from IP {}",
        "Failed login attempt for user '{}' from IP {}",
        "Service '{}' started successfully",
        "Service '{}' stopped unexpectedly",
        "Disk volume '{}' if 90% full",
        "Application '{}' crashed with error code 0x{:04X}",
        "Scheduled task '{}' completed with warnings",
        "Network connection from {} to {} established",
        "Security policy updated: {}",
        "System shutdown initiated by user '{}'"
    ]
    
    # Имена пользователей для примера
    USERS = ["admin", "teacher_popov", "student_timaty", "root", "svc_moodle", "svc_1c"]
    
    # Айпи адреса для примера
    IPS = ["192.168.1.{}".format(i) for i in range(10,50)]
    
    # Примеры рабочих сервисов
    SERVICES = ["Moodle", "1C:Enterprise", "PostgreSQL", "Apache2", "SMB", "DHCP"]
    
    @classmethod
    # Генерирует одну строку лога в формате json
    def generate_log(cls) -> Dict[str, str]:
        timestamp = datetime.now().isoformat()
        
        # 80 проц шанс обычного лога, 20 проц - ошибка или фатальное событие
        log_type = random.choices(["info", "warning", "error"], weights=[0.6, 0.2, 0.2])[0]
        
        # Списки для реалистичных данных
        users = ["admin", "teacher_ivanov", "student_petrov", "root", "svc_moodle", "svc_1c"]
        ips = ["192.168.1.{}".format(i) for i in range(10, 50)]
        services = ["Moodle", "1C:Enterprise", "PostgreSQL", "Apache2", "SMB", "DHCP"]
        apps = ["chrome.exe", "explorer.exe", "1c.exe", "moodle.exe", "postgres.exe"]
        disks = ["C:", "D:", "/dev/sda1", "/dev/sdb1"]
        tasks = ["Backup", "Update", "Cleanup", "DailyReport"]
        policies = ["Password policy", "Audit policy", "User rights", "Firewall rules"]
        ips_external = ["8.8.8.8", "10.0.0.1", "192.168.1.1", "1.1.1.1"]
        
        user = random.choice(users)
        ip = random.choice(ips)
        service = random.choice(services)
        app = random.choice(apps)
        disk = random.choice(disks)
        task = random.choice(tasks)
        policy = random.choice(policies)
        ip_ext = random.choice(ips_external)
        error_code = random.randint(0x0001, 0xFFFF)
        event_id = random.randint(1000, 9999)
        
        # Выбираем случайный тип события и генерируем сообщение
        event_type = random.choice([
            "user_login",
            "user_login_failed",
            "service_start",
            "service_stop",
            "disk_full",
            "app_crash",
            "scheduled_task",
            "network_connection",
            "security_policy",
            "system_shutdown"
        ])
        
        # Генерируем параметры в зависимости от типа
        if event_type == "user_login":
            message = f"User '{user}' logged in successfully from IP {ip}"
        elif event_type == "user_login_failed":
            message = f"Failed login attempt for user '{user}' from IP {ip}"
        elif event_type == "service_start":
            message = f"Service '{service}' started successfully"
        elif event_type == "service_stop":
            message = f"Service '{service}' stopped unexpectedly"
        elif event_type == "disk_full":
            message = f"Disk volume '{disk}' is {random.randint(85, 99)}% full"
        elif event_type == "app_crash":
            message = f"Application '{app}' crashed with error code 0x{error_code:04X}"
        elif event_type == "scheduled_task":
            message = f"Scheduled task '{task}' completed with warnings"
        elif event_type == "network_connection":
            message = f"Network connection from {ip} to {ip_ext} established"
        elif event_type == "security_policy":
            message = f"Security policy updated: {policy}"
        elif event_type == "system_shutdown":
            message = f"System shutdown initiated by user '{user}'"
        else:
            message = f"System event {event_id}: {event_type} occurred"
            
        # Формируем структуру, похожую на Windows Event
        log_entry = {
            "timestamp": timestamp,
            "host": random.choice(["WIN-SRV-01", "WIN-SRV-02", "WIN-1C-SRV"]),
            "source": "Windows Event Log",
            "event_id": random.randint(1000, 9999),
            "level": log_type,
            "user": user,
            "message": message,
            "raw": f"<Event><System><EventID>{random.randint(1000,9999)}</EventID>"
                   f"<TimeCreated SystemTime='{timestamp}'/></System>"
                   f"<EventData>{message}</EventData></Event>"
        }
        
        return log_entry
        
    @classmethod
    # Генерирует пачку логов
    def generate_batch(cls, count: int = 10) -> List[Dict[str, str]]:
        return [cls.generate_log() for _ in range (count)]
        
# Пример использования (для теста)
if __name__ == "__main__":
    print("🧪 Генерация тестовых Windows-логов:")
    for log in WindowsLogEmulator.generate_batch(5):
        print(json.dumps(log, indent=2, ensure_ascii=False))
        print("-" * 50)
