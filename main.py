import os
import asyncio
import logging
import sys
import shutil
import time
from telethon import TelegramClient, events, errors
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# =============================================================================
# CONFIGURAÇÕES DO BOT E PARÂMETROS GERAIS
# =============================================================================
api_id = '25600801'
api_hash = '20b2f83fbae27a8f6d2fa650228d0ff9'
BOT_TOKEN = '7795800821:AAGfbyUWAJbdSB2OWfoZiMu0l22Waje1yWo'   # Fornecido pelo @BotFather

# Canal de destino (fixo)
canal_destino = 'https://t.me/+kz7sg6QI8S02ZmUx'

# Diretório para salvar os arquivos baixados temporariamente
pasta_download = 'downloads'
if not os.path.exists(pasta_download):
    os.makedirs(pasta_download)

# Parâmetros de delay e tamanho de pasta
user_delay_seconds = 10
bot_delay_seconds = 1.2
skip_delay_seconds = 1
cache_folder_max_size_mb = 5000

# Arquivo para armazenar IDs de mensagens já processadas
processed_messages_file = 'processed_messages.txt'
file_lock = asyncio.Lock()

# Controle de mensagens processadas (carregado do arquivo)
if os.path.exists(processed_messages_file):
    with open(processed_messages_file, 'r') as f:
        processed_ids = set(int(line.strip()) for line in f if line.strip())
else:
    processed_ids = set()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("transfer_log.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Inicia cliente do Telethon como BOT
client = TelegramClient('bot_session', api_id, api_hash)

# Dicionário para rastrear a última atualização de progresso por mensagem
last_progress_update = {}

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def verificar_tamanho_pasta(pasta, tamanho_max_mb):
    """
    Verifica o tamanho total da pasta de downloads e, se exceder o limite,
    faz a limpeza completa.
    """
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


def progress_display(current, total, prefix='', message_id=None):
    """
    Exibe ou loga o progresso (download/upload) de um arquivo,
    atualizando a cada X segundos para não gerar muito log.
    """
    now = time.time()
    key = f"{prefix}_{message_id}"
    # Aqui, atualiza a cada 2 segundos, por exemplo
    if key not in last_progress_update or now - last_progress_update[key] >= 2:
        percent = (current / total) * 100 if total else 0
        logger.info(f"{prefix}Progresso da mensagem {message_id}: {percent:.2f}% ({current} de {total} bytes)")
        last_progress_update[key] = now


def retry_decorator():
    """
    Decorador com Tenacity para tentar novamente em casos de FloodWait,
    RPCError ou TimeoutError.
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
        retry=retry_if_exception_type((errors.FloodWaitError, errors.RPCError, asyncio.TimeoutError))
    )


@retry_decorator()
async def processar_mensagem(mensagem, entidade_destino, semaphore):
    """
    Processa uma única mensagem: se tiver mídia, baixa e envia;
    se for texto, reenvia como texto.
    """
    async with semaphore:
        try:
            # Se a mensagem já foi processada, sai
            if mensagem.id in processed_ids:
                return

            if mensagem.media:
                # Define o nome do arquivo
                nome_arquivo = mensagem.file.name if mensagem.file and mensagem.file.name else f'arquivo_{mensagem.id}'
                caminho_arquivo = os.path.join(pasta_download, nome_arquivo)

                # Verifica e limpa a pasta se necessário
                verificar_tamanho_pasta(pasta_download, cache_folder_max_size_mb)

                # Faz download com callback de progresso
                await mensagem.download_media(
                    file=caminho_arquivo,
                    progress_callback=lambda current, total: progress_display(
                        current, total, f"Baixando {nome_arquivo}: ", mensagem.id
                    )
                )
                logger.info(f"Baixado: {caminho_arquivo}")

                # Breve delay para respeitar limites
                await asyncio.sleep(bot_delay_seconds)

                # Envia o arquivo ao canal de destino
                await client.send_file(
                    entidade_destino,
                    caminho_arquivo,
                    caption=mensagem.message or '',
                    progress_callback=lambda current, total: progress_display(
                        current, total, f"Enviando {nome_arquivo}: ", mensagem.id
                    )
                )
                logger.info(f"Enviado para o canal de destino: {caminho_arquivo}")

                # Outro delay pós-envio
                await asyncio.sleep(bot_delay_seconds)

                # Remove o arquivo baixado
                os.remove(caminho_arquivo)
                logger.info(f"Removido arquivo local: {caminho_arquivo}")

            elif mensagem.message:
                # Se for apenas texto, envia como texto
                await client.send_message(entidade_destino, mensagem.message)
                logger.info(f"Mensagem de texto enviada (ID {mensagem.id}): {mensagem.message}")
                await asyncio.sleep(bot_delay_seconds)

            # Marca a mensagem como processada
            async with file_lock:
                with open(processed_messages_file, 'a') as f:
                    f.write(f"{mensagem.id}\n")
                processed_ids.add(mensagem.id)

        except errors.FloodWaitError as e:
            logger.warning(f"FloodWaitError: Esperando {e.seconds} segundos.")
            await asyncio.sleep(e.seconds)
            raise e  # Re-lança para tentar novamente (tenacity)
        except Exception as e:
            logger.error(f"Erro ao processar a mensagem {mensagem.id}: {e}")
            raise e


# =============================================================================
# EVENTOS DO BOT
# =============================================================================

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    """
    Comando /start para iniciar a conversa.
    """
    await event.respond(
        "Olá! Envie o link do canal de origem (por exemplo, https://t.me/xxx ou @xxx) "
        "para que eu possa começar a transferir as mensagens para o canal de destino fixo."
    )


@client.on(events.NewMessage)
async def receber_link(event):
    """
    Recebe mensagens do usuário. Se for um link de canal,
    inicia a transferência de mensagens.
    """
    # Ignora se for comando /start, pois já tratado acima
    if event.text.startswith('/start'):
        return

    # O que vier aqui, vamos assumir que seja o link do canal de origem
    canal_origem = event.text.strip()

    # Verifica se é plausivelmente um link/username de canal
    if ('t.me/' not in canal_origem) and (not canal_origem.startswith('@')):
        await event.respond("Isso não parece ser um link/username de canal válido. Tente novamente.")
        return

    await event.respond(f"Recebi o canal de origem: {canal_origem}. Iniciando a transferência...")
    logger.info(f"Usuário pediu para transferir do canal: {canal_origem}")

    try:
        # Obtém a entidade do canal de origem e de destino
        entidade_origem = await client.get_entity(canal_origem)
        entidade_destino = await client.get_entity(canal_destino)
    except ValueError as ve:
        logger.error(f"Erro ao obter as entidades dos canais: {ve}")
        await event.respond(f"Erro ao acessar o canal: {ve}")
        return
    except errors.UsernameNotOccupiedError as e:
        logger.error(f"Username inválido fornecido: {e}")
        await event.respond("O link/username do canal de origem é inválido ou não existe.")
        return
    except errors.ChannelPrivateError as e:
        logger.error(f"Canal privado ou sem acesso: {e}")
        await event.respond("O canal de origem é privado ou não tenho acesso.")
        return
    except Exception as e:
        logger.error(f"Erro desconhecido ao obter entidades: {e}")
        await event.respond(f"Erro desconhecido: {e}")
        return

    # Limita o número de tarefas simultâneas
    semaphore = asyncio.Semaphore(1)

    contador = 0
    total_mensagens = 0

    # Primeiro, calcula quantas mensagens há para processar
    await event.respond("Calculando quantidade de mensagens...")
    async for _ in client.iter_messages(entidade_origem):
        total_mensagens += 1

    # Se não encontrou mensagens, encerra
    if total_mensagens == 0:
        await event.respond("Não há mensagens nesse canal para transferir.")
        return

    await event.respond(f"Encontrei {total_mensagens} mensagens. Iniciando processamento...")

    # Lista de tarefas
    tarefas = []

    # Processa as mensagens (do mais antigo para o mais novo)
    async for mensagem in client.iter_messages(entidade_origem, reverse=True):
        if mensagem.id in processed_ids:
            continue

        # Cria tarefa
        tarefa = asyncio.create_task(processar_mensagem(mensagem, entidade_destino, semaphore))
        tarefas.append(tarefa)

        contador += 1
        # A cada 10 mensagens (por exemplo), manda update
        if contador % 10 == 0:
            porcentagem = (contador / total_mensagens) * 100
            await event.respond(
                f"Progresso: {contador}/{total_mensagens} mensagens ({porcentagem:.2f}%)."
            )
            # Pequeno delay para não atropelar
            await asyncio.sleep(user_delay_seconds)

    # Aguarda todas as tarefas finalizarem
    results = await asyncio.gather(*tarefas, return_exceptions=True)

    # Verifica se houve exceções
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Exceção capturada: {r}")

    await event.respond("Todas as mensagens do canal informado foram transferidas com sucesso!\n"
                        "Se quiser processar outro canal de origem, envie o novo link agora.")


# =============================================================================
# FUNÇÃO MAIN
# =============================================================================

async def main():
    # Inicia o bot
    await client.start(bot_token=BOT_TOKEN)
    logger.info("Bot iniciado e aguardando mensagens...")

    # Mantém o bot rodando
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado no bot: {e}")
