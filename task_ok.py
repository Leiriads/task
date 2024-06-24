from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor
import time

# Variável de buffer para adicionar ao tempo de execução excedido
buffer_time = 10

# Constantes para controle de agendamento
TASK1_ENABLED = 1
TASK2_ENABLED = 1
TASK3_ENABLED = 1
TASK4_ENABLED = 1
TASK5_ENABLED = 1

class DynamicSchedulerTask:
    def __init__(self, scheduler, executor, func, fixed_interval, enabled, task_id=None):
        self.scheduler = scheduler
        self.executor = executor
        self.func = func
        self.fixed_interval = fixed_interval
        self.interval = fixed_interval
        self.enabled = enabled
        self.task_id = task_id or func.__name__

    def run_task(self):
        if not self.enabled:
            return
        future = self.executor.submit(self._execute)
        future.add_done_callback(self._task_done)

    def _execute(self):
        start_time = time.time()
        print(f"Iniciando a execução da tarefa {self.task_id}")  # Debug
        self.func()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Tarefa {self.task_id} concluída em {execution_time:.2f} segundos")  # Debug
        return execution_time

    def _task_done(self, future):
        execution_time = future.result()
        self.adjust_schedule(execution_time)

    def adjust_schedule(self, execution_time):
        if execution_time > self.fixed_interval:
            self.interval = execution_time + buffer_time
            print(f"Tarefa {self.task_id} excedeu o tempo fixo. Ajustando para {self.interval} segundos")  # Debug
        else:
            self.interval = self.fixed_interval
            print(f"Tarefa {self.task_id} dentro do tempo fixo. Mantendo {self.interval} segundos")  # Debug
        self.scheduler.reschedule_job(self.task_id, trigger='interval', seconds=self.interval)

    def schedule_task(self):
        if self.enabled:
            self.scheduler.add_job(self.run_task, 'interval', seconds=self.fixed_interval, id=self.task_id)
            print(f"Tarefa {self.task_id} agendada com intervalo de {self.fixed_interval} segundos")  # Debug

# Funções de exemplo
def task1():
    print('iniciando func 1')
    time.sleep(70)  # Simula uma execução longa (70 segundos)

def task2():
    print('iniciando func 2')
    time.sleep(80)  # Simula uma execução longa (80 segundos)

def task3():
    time.sleep(20)  # Simula uma execução longa (20 segundos)

def task4():
    time.sleep(25)  # Simula uma execução longa (25 segundos)

def task5():
    time.sleep(30)  # Simula uma execução longa (30 segundos)

# Configura o scheduler
scheduler = BackgroundScheduler()

# Executor para rodar as tarefas em paralelo
executor = ThreadPoolExecutor(max_workers=5)  # Define o número de threads conforme necessário

# Cria e agenda as tarefas com intervalos fixos e verificação de habilitação
tasks = [
    DynamicSchedulerTask(scheduler, executor, task1, 60, TASK1_ENABLED),  # Intervalo fixo de 60 segundos (1 minuto)
    DynamicSchedulerTask(scheduler, executor, task2, 60, TASK2_ENABLED),  # Intervalo fixo de 60 segundos
    DynamicSchedulerTask(scheduler, executor, task3, 20, TASK3_ENABLED),  # Intervalo fixo de 20 segundos
    DynamicSchedulerTask(scheduler, executor, task4, 25, TASK4_ENABLED),  # Intervalo fixo de 25 segundos
    DynamicSchedulerTask(scheduler, executor, task5, 40, TASK5_ENABLED),  # Intervalo fixo de 40 segundos
]

for task in tasks:
    task.schedule_task()

scheduler.start()
print("Scheduler iniciado")  # Debug

# Mantém o script rodando
try:
    while True:
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    executor.shutdown(wait=True)
    print("Scheduler finalizado")  # Debug
