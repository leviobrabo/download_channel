import os
import asyncio
import logging
import sys
import shutil
import time
from telethon import TelegramClient, events, errors
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# =============================================================================
# CONFIGURAÇÕES DA CONTA DE USUÁRIO (USERBOT)
# =============================================================================
user_api_id = '25600801'
user_api_hash = '20b2f83fbae27a8f6d2fa650228d0ff9'
user_phone = '+5571988130989'

# =============================================================================
# CONFIGURAÇÕES DO BOT
# =============================================================================
bot_api_id  = '25600801'
bot_api_hash = '20b2f83fbae27a8f6d2fa650228d0ff9'
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

# Cria as sessões separadas
userbot_session = 'userbot_session'
bot_session = 'bot_session'

# Clientes do Telethon
userbot_client = TelegramClient(userbot_session, user_api_id, user_api_hash)
bot_client = TelegramClient(bot_session, bot_api_id, bot_api_hash)


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
        logger.info(f"Tamanho da pasta de cache excedeu {tamanho_max_mb} MB. Iniciando limpeza.")
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
        retry=retry_if_exception_type(
            (errors.FloodWaitError, errors.RPCError, asyncio.TimeoutError)
        )
    )


@retry_decorator()
async def processar_mensagem(mensagem, entidade_destino, semaphore):
    """
    Processa uma única mensagem (userbot).
    Se tiver mídia, baixa e envia; se for texto, reenvia como texto.
    Usa `processed_messages_file` para evitar duplicados.
    """
    async with semaphore:
        try:
            if mensagem.id in processed_ids:
                return

            if mensagem.media:
                nome_arquivo = mensagem.file.name if mensagem.file and mensagem.file.name else f'arquivo_{mensagem.id}'
                caminho_arquivo = os.path.join(pasta_download, nome_arquivo)

                verificar_tamanho_pasta(pasta_download, cache_folder_max_size_mb)

                # Download (pelo userbot)
                await mensagem.download_media(
                    file=caminho_arquivo,
                    progress_callback=lambda current, total: progress_display(
                        current, total, f"[Userbot] Baixando {nome_arquivo}: ", mensagem.id
                    )
                )
                logger.info(f"[Userbot] Baixado: {caminho_arquivo}")

                # Pequeno delay
                await asyncio.sleep(bot_delay_seconds)

                # Envia ao canal de destino (pelo userbot)
                await userbot_client.send_file(
                    entidade_destino,
                    caminho_arquivo,
                    caption=mensagem.message or '',
                    progress_callback=lambda current, total: progress_display(
                        current, total, f"[Userbot] Enviando {nome_arquivo}: ", mensagem.id
                    )
                )
                logger.info(f"[Userbot] Enviado para o canal de destino: {caminho_arquivo}")

                # Apaga arquivo local
                os.remove(caminho_arquivo)
                logger.info(f"[Userbot] Removido arquivo local: {caminho_arquivo}")

            elif mensagem.message:
                # Mensagem de texto
                await userbot_client.send_message(entidade_destino, mensagem.message)
                logger.info(f"[Userbot] Mensagem de texto enviada (ID {mensagem.id}): {mensagem.message}")

            # Marca como processada
            async with file_lock:
                with open(processed_messages_file, 'a') as f:
                    f.write(f"{mensagem.id}\n")
                processed_ids.add(mensagem.id)

        except errors.FloodWaitError as e:
            logger.warning(f"[Userbot] FloodWaitError: Esperando {e.seconds} segundos.")
            await asyncio.sleep(e.seconds)
            raise e
        except Exception as e:
            logger.error(f"[Userbot] Erro ao processar a mensagem {mensagem.id}: {e}")
            raise e


async def transferir_canal(canal_origem, user_chat_id):
    """
    Faz a transferência das mensagens do `canal_origem` para o `canal_destino`.
    - Lê o canal de origem (pelo userbot)
    - Envia para o canal destino (userbot)
    - Envia status pelo bot para o usuário que digitou o link.
    """
    # Tenta obter entidades pelo userbot
    try:
        entidade_origem = await userbot_client.get_entity(canal_origem)
    except Exception as e:
        erro = f"Erro ao acessar canal de origem '{canal_origem}': {e}"
        logger.error(erro)
        await bot_client.send_message(user_chat_id, erro)
        return

    try:
        entidade_destino = await userbot_client.get_entity(canal_destino)
    except Exception as e:
        erro = f"Erro ao acessar canal de destino '{canal_destino}': {e}"
        logger.error(erro)
        await bot_client.send_message(user_chat_id, erro)
        return

    # Contar total de mensagens (só pra status)
    await bot_client.send_message(user_chat_id, "[Userbot] Contando mensagens no canal de origem...")
    total_mensagens = 0
    async for _ in userbot_client.iter_messages(entidade_origem):
        total_mensagens += 1

    if total_mensagens == 0:
        await bot_client.send_message(user_chat_id, "Não há mensagens nesse canal para transferir.")
        return

    await bot_client.send_message(
        user_chat_id,
        f"Encontradas {total_mensagens} mensagens. Iniciando envio..."
    )

    # Para evitar sobrecarga
    semaphore = asyncio.Semaphore(1)

    contador = 0
    tarefas = []
    async for mensagem in userbot_client.iter_messages(entidade_origem, reverse=True):
        # Se já processamos, pula
        if mensagem.id in processed_ids:
            continue

        tarefa = asyncio.create_task(processar_mensagem(mensagem, entidade_destino, semaphore))
        tarefas.append(tarefa)

        contador += 1
        # A cada 10 msgs, manda update
        if contador % 10 == 0:
            porcentagem = (contador / total_mensagens) * 100
            await bot_client.send_message(
                user_chat_id,
                f"Progresso: {contador}/{total_mensagens} mensagens ({porcentagem:.2f}%)."
            )
            await asyncio.sleep(user_delay_seconds)

    # Aguarda finalização de tudo
    results = await asyncio.gather(*tarefas, return_exceptions=True)

    # Verifica se houve exceções
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"[Userbot] Exceção capturada no gather: {r}")

    await bot_client.send_message(
        user_chat_id,
        f"Todas as {contador} mensagens foram transferidas com sucesso!"
    )


# =============================================================================
# EVENTOS DO BOT (RECEBE LINK E PASSA PARA USERBOT)
# =============================================================================

@bot_client.on(events.NewMessage(pattern='/start'))
async def start_cmd(event):
    await event.respond(
        "Olá! Envie o link do canal de origem (por exemplo, https://t.me/xxx ou @xxx)."
        "\nO userbot fará a transferência para o canal de destino pré-configurado."
    )

@bot_client.on(events.NewMessage)
async def receber_link(event):
    """
    Se não for /start, consideramos que o texto é o link do canal de origem.
    """
    if event.text.startswith('/start'):
        return

    canal_origem = event.text.strip()
    chat_id = event.chat_id

    await event.respond(f"Recebi o canal de origem: {canal_origem}. Tentando iniciar transferência...")
    asyncio.create_task(transferir_canal(canal_origem, chat_id))


# =============================================================================
# FUNÇÃO MAIN
# =============================================================================
async def main():
    # ---------------------------------------------------------------------
    # 1) Inicia o userbot (conta de usuário)
    #    Se for a primeira vez, pedirá código SMS no console.
    # ---------------------------------------------------------------------
    await userbot_client.start(phone=user_phone)
    logger.info("[Userbot] Conectado com sucesso.")

    # ---------------------------------------------------------------------
    # 2) Inicia o bot
    # ---------------------------------------------------------------------
    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("[Bot] Iniciado e aguardando comandos...")

    # Mantém ambos rodando
    await bot_client.run_until_disconnected()
    # Se preferir, pode usar asyncio.gather para aguardar ambos:
    # await asyncio.gather(
    #     bot_client.run_until_disconnected(),
    #     userbot_client.run_until_disconnected()
    # )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Encerrado pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
