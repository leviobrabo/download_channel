import os
import asyncio
import logging
import sys
import shutil
import time
from telethon import TelegramClient, errors
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Configurações do Script
api_id = '25600801'
api_hash = '20b2f83fbae27a8f6d2fa650228d0ff9'
telefone = '+5571988130989'  # Inclua o código do país, ex: +5511999999999

# Links dos Canais de Origem e Destino
canal_origem = 'https://t.me/+BUKu5dBedWEzYzVh'
canal_destino = 'https://t.me/+kz7sg6QI8S02ZmUx'

# Diretório para Salvar os Arquivos Baixados Temporariamente
pasta_download = 'downloads'
if not os.path.exists(pasta_download):
    os.makedirs(pasta_download)

# Parâmetros de Configuração
user_delay_seconds = 10
bot_delay_seconds = 1.2
skip_delay_seconds = 1
cache_folder_max_size_mb = 5000
auto_restart_min = 60  # em minutos

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("transfer_log.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Inicializa o Cliente do Telegram
client = TelegramClient('session_name', api_id, api_hash)

# Lock para operações de arquivo
file_lock = asyncio.Lock()

# Arquivo para armazenar IDs de mensagens processadas
processed_messages_file = 'processed_messages.txt'

# Carrega os IDs das mensagens já processadas
if os.path.exists(processed_messages_file):
    with open(processed_messages_file, 'r') as f:
        processed_ids = set(int(line.strip()) for line in f if line.strip())
else:
    processed_ids = set()

# Dicionário para rastrear a última atualização de progresso
last_progress_update = {}

# Função para mostrar o progresso
def progress_display(current, total, prefix='', message_id=None):
    now = time.time()
    key = f"{prefix}_{message_id}"
    if key not in last_progress_update or now - last_progress_update[key] >= 10:  # Atualiza a cada 1 segundo
        percent = (current / total) * 100 if total else 0
        logger.info(f"{prefix}Progresso da mensagem {message_id}: {percent:.2f}% ({current} de {total} bytes)")
        last_progress_update[key] = now

# Função para Verificar e Limpar o Tamanho da Pasta de Download
def verificar_tamanho_pasta(pasta, tamanho_max_mb):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(pasta):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    tamanho_mb = total_size / (1024 * 1024)
    if tamanho_mb > tamanho_max_mb:
        logger.info(f"Tamanho da pasta de cache excedeu {cache_folder_max_size_mb} MB. Iniciando limpeza.")
        shutil.rmtree(pasta)
        os.makedirs(pasta)
        logger.info("Pasta de cache limpa com sucesso.")

# Função de Retry com Tenacity
def retry_decorator():
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
        retry=retry_if_exception_type((errors.FloodWaitError, errors.RPCError, asyncio.TimeoutError))
    )

# Função para Processar uma Única Mensagem
@retry_decorator()
async def processar_mensagem(mensagem, entidade_destino, semaphore):
    async with semaphore:
        try:
            if mensagem.media:
                # Define o Caminho Completo para Salvar o Arquivo
                nome_arquivo = mensagem.file.name if mensagem.file and mensagem.file.name else f'arquivo_{mensagem.id}'
                caminho_arquivo = os.path.join(pasta_download, nome_arquivo)

                # Verifica o Tamanho da Pasta de Download
                verificar_tamanho_pasta(pasta_download, cache_folder_max_size_mb)

                # Faz o Download do Arquivo com Barra de Progresso
                await mensagem.download_media(
                    file=caminho_arquivo,
                    progress_callback=lambda current, total: progress_display(current, total, f"Baixando {nome_arquivo}: ", mensagem.id)
                )
                logger.info(f"Baixado: {caminho_arquivo}")

                # Delay para Respeitar Limites da API
                await asyncio.sleep(bot_delay_seconds)

                # Envia o Arquivo para o Canal de Destino com a Legenda Original, se Houver, com Barra de Progresso
                await client.send_file(
                    entidade_destino,
                    caminho_arquivo,
                    caption=mensagem.message or '',
                    progress_callback=lambda current, total: progress_display(current, total, f"Enviando {nome_arquivo}: ", mensagem.id)
                )
                logger.info(f"Enviado para o canal de destino: {caminho_arquivo}")

                # Delay Pós Upload
                await asyncio.sleep(bot_delay_seconds)

                # Remove o Arquivo Após o Upload para Liberar Espaço
                os.remove(caminho_arquivo)
                logger.info(f"Removido: {caminho_arquivo}")

            elif mensagem.message and not mensagem.media:
                # Envia a Mensagem de Texto para o Canal de Destino
                await client.send_message(entidade_destino, mensagem.message)
                logger.info(f"Mensagem de texto enviada: {mensagem.message}")

                # Delay Pós Envio
                await asyncio.sleep(bot_delay_seconds)

            # Após processar com sucesso, registra o ID da mensagem
            async with file_lock:
                with open(processed_messages_file, 'a') as f:
                    f.write(f"{mensagem.id}\n")
                processed_ids.add(mensagem.id)

        except errors.FloodWaitError as e:
            logger.warning(f"FloodWaitError: Esperando {e.seconds} segundos.")
            await asyncio.sleep(e.seconds)
            raise e  # Re-raise para acionar o retry
        except Exception as e:
            logger.error(f"Erro ao processar a mensagem {mensagem.id}: {e}")
            raise e  # Re-raise para acionar o retry

# Função Principal
async def main():
    await client.start(phone=telefone)
    logger.info("Conectado ao Telegram")

    try:
        # Obtém as Entidades dos Canais (Funciona com Links e IDs)
        entidade_origem = await client.get_entity(canal_origem)
        entidade_destino = await client.get_entity(canal_destino)
    except ValueError as ve:
        logger.error(f"Erro ao obter as entidades dos canais: {ve}")
        return
    except errors.UsernameNotOccupiedError as e:
        logger.error(f"Username inválido fornecido: {e}")
        return
    except errors.ChannelPrivateError as e:
        logger.error(f"Canal privado ou sem acesso: {e}")
        return
    except Exception as e:
        logger.error(f"Erro desconhecido ao obter entidades: {e}")
        return

    # Limita o Número de Tarefas Simultâneas para Evitar Sobrecarga
    semaphore = asyncio.Semaphore(1)  # Ajuste conforme necessário

    contador = 0
    tarefas = []

    logger.info("Iniciando transferência de mensagens...")

    async for mensagem in client.iter_messages(entidade_origem, reverse=True):  # reverse=True para processar do mais antigo ao mais novo
        if mensagem.id in processed_ids:
            logger.info(f"Mensagem {mensagem.id} já processada, pulando.")
            continue

        tarefa = asyncio.create_task(processar_mensagem(mensagem, entidade_destino, semaphore))
        tarefas.append(tarefa)
        contador += 1

        if contador % 100 == 0:
            logger.info(f"{contador} mensagens adicionadas para processamento...")
            await asyncio.sleep(user_delay_seconds)  # Delay para evitar sobrecarga

    # Aguarda Todas as Tarefas Terminarem
    await asyncio.gather(*tarefas, return_exceptions=True)

    logger.info("Transferência concluída.")

if __name__ == "__main__":
    try:
        # Executa a função principal
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado no script: {e}")
