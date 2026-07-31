import os
import logging
from typing import List, Dict, Optional

"""
import winrm
from winrm.exceptions import WinRMError

# Сбор логов с windows-серверов через WinRM
class WinRMCollector:
    def __init__(self, host: str, username: str, password: str, domain: Optional[str] = None):
        self.host = host
        self.username = username
        self.password = password
        self.domain = domain
        self.session = None

    # Установка WinRM соединения. Для продакшн используется https с проверкой сертификата
    def connect(self) -> bool:
        try:
            self.session = winrm.Session(
                self.host,
                auth=(self.username, self.password),
                transport='ntlm' if self.domain else 'plaintext',
                server_cert_validation='ignore'  # Только для тестов
            )
            # Проверка соединения
            result = self.session.run_cmd('whoami')
            if result.status_code == 0:
                logging.info(f"✅ Успешное подключение к {self.host}")
                return True
            else:
                logging.error(f"❌ Ошибка подключения к {self.host}: {result.std_err}")
                return False
        except WinRMError as e:
            logging.error(f"❌ WinRM ошибка для {self.host}: {str(e)}")
            return False

    # Получение логов из Windows Event Log. Powershell команда для получения событий. Используем Get-WinEvent для большей производительности
    def get_event_logs(self, log_type: str = "Application", hours_back: int = 1) -> List[Dict]:
        ps_script = f"""
        $startTime = $endTime.AddHours(-{hours_back})
        $events = Get-WinEvent -LogName {log_type} -MaxEvents 100 -FilterXPath @'
        <QueryList>
            <Query Id="0" Path="{log_type}">
                <Select Path="{log_type}">
                    *[System[TimeCreated[@SystemTime >= '{$startTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")}']]]
                </Select>
            </Query>
        </QueryList>
        '@
        $events | ForEach-Object {{
            [PSCustomObject]@{{
                TimeCreated = $_.TimeCreated
                Id = $_.Id
                Level = $_.Level
                Provider = $_.ProviderName
                Message = $_.Message
                User = $_.UserId
                MachineName = $_.MachineName
            }}
        }} | ConvertTo-Json -Compress
        """

        try:
            result = self.session.run_ps(ps_script)
            if result.status_code == 0 and result.std_out:
                import json
                logs = json.loads(result.std_out)
                # Если вернулся один объект, оборачивается в список
                if isinstance(logs, dict):
                    logs = [logs]
                return logs
            else:
                logging.error(f"❌ Ошибка чтения логов с {self.host}: {result.std_err}")
                return []
        except Exception as e:
            logging.error(f"❌ Исключение при чтении логов с {self.host}: {str(e)}")
            return []

    # Специлизировання функция для Security логов - логины, доступы
    def get_security_logs(self, hours_back: int = 1) -> List[Dict]:
        return self.get_event_logs("Security", hours_back)

    # Системные логи - ошибки дисков, драйверов
    def get_system_logs(self, hours_back: int = 1) -> List[Dict]:
        return self.get_event_logs("System", hours_back)

    # Закрытие соединения
    def disconnect(self):
        self.session = None
        logging.info(f"🔒 Соединение с {self.host} закрыто")

# !!! ВНИМАНИЕ: Для production используйте этот код только после настройки:
# 1. Установите pywinrm: pip install pywinrm
# 2. Настройте WinRM на Windows-серверах:
#    winrm quickconfig
#    winrm set winrm/config/service/auth '@{Basic="true"}'
#    winrm set winrm/config/service '@{AllowUnencrypted="false"}'
# 3. Используйте HTTPS и сертификаты!
"""
