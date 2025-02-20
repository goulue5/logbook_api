import asyncio
import base64
import json
import signal
import sys
from datetime import time
from typing import Literal, TypedDict

import pyaudio
import requests
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosedOK

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
import io
import wave
import datetime

from supabase import create_client, Client

# --- Supabase configuration ---
SUPABASE_URL = "https://myjauxextjrucjpkskky.supabase.co/"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im15amF1eGV4dGpydWNqcGtza2t5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTc4NjY1NiwiZXhwIjoyMDU1MzYyNjU2fQ.YjWr3Zp3spPFo4PbRvzggo0zm0MNhXyB_NJepOuhhYY"
SUPABASE_BUCKET_NAME = "logbook-audio-records"
SUPABASE_TABLE_NAME = "recordings"

supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

app = FastAPI()

## Constants
GLADIA_API_URL = "https://api.gladia.io"

## Type definitions
class InitiateResponse(TypedDict):
    id: str
    url: str

class LanguageConfiguration(TypedDict):
    languages: list[str] | None
    code_switching: bool | None

class StreamingConfiguration(TypedDict):
    encoding: Literal["wav/pcm", "wav/alaw", "wav/ulaw"]
    bit_depth: Literal[8, 16, 24, 32]
    sample_rate: Literal[8_000, 16_000, 32_000, 44_100, 48_000]
    channels: int
    language_config: LanguageConfiguration | None

## Helpers
def get_gladia_key() -> str:
    return "24e46562-4fb0-4b7c-aa73-70bbbf68330d"

def init_live_session(config: StreamingConfiguration) -> InitiateResponse:
    gladia_key = get_gladia_key()
    response = requests.post(
        f"{GLADIA_API_URL}/v2/live",
        headers={"X-Gladia-Key": gladia_key},
        json=config,
        timeout=3,
    )
    if not response.ok:
        print(f"{response.status_code}: {response.text or response.reason}")
        raise Exception(f"Gladia API Error: {response.status_code} - {response.text}")
    return response.json()

def format_duration(seconds: float) -> str:
    milliseconds = int(seconds * 1_000)
    return time(
        hour=milliseconds // 3_600_000,
        minute=(milliseconds // 60_000) % 60,
        second=(milliseconds // 1_000) % 60,
        microsecond=milliseconds % 1_000 * 1_000,
    ).isoformat(timespec="milliseconds")

async def print_messages_from_socket(socket: ClientConnection, websocket: WebSocket, transcription_list: list) -> None:
    try:
        async for message in socket:
            content = json.loads(message)
            if content["type"] == "transcript" and content["data"]["is_final"]:
                start = format_duration(content["data"]["utterance"]["start"])
                end = format_duration(content["data"]["utterance"]["end"])
                text = content["data"]["utterance"]["text"].strip()
                formatted_text = f"{start} --> {end} | {text}"
                print(formatted_text)
                try:
                    await websocket.send_text(formatted_text)
                except RuntimeError as e:
                    print(f"Erreur lors de l'envoi de la transcription: {e}")
                    return
                transcription_list.append(formatted_text)

            if content["type"] == "post_final_transcript":
                print("\n################ End of session ################\n")
                print(json.dumps(content, indent=2, ensure_ascii=False))
                try:
                    await websocket.send_text("\n################ End of session ################\n")
                except RuntimeError as e:
                    print(f"Erreur lors de l'envoi de la fin de session: {e}")
                    return

    except WebSocketDisconnect:
        print("Client disconnected")
        return
    except Exception as e:
        print(f"Erreur dans print_messages_from_socket: {e}")

async def stop_recording(websocket: ClientConnection) -> None:
    print(">>>>> Ending the recording…")
    await websocket.send(json.dumps({"type": "stop_recording"}))
    await asyncio.sleep(0)

## Sample code
P = pyaudio.PyAudio()

CHANNELS = 1
FORMAT = pyaudio.paInt16
FRAMES_PER_BUFFER = 3200
SAMPLE_RATE = 16_000

STREAMING_CONFIGURATION: StreamingConfiguration = {
    "encoding": "wav/pcm",
    "sample_rate": SAMPLE_RATE,
    "bit_depth": 16,
    "channels": CHANNELS,
    "language_config": {
        "languages": [],
        "code_switching": True,
    },
}

async def send_audio(socket: ClientConnection, audio_data: io.BytesIO) -> None:
    stream = P.open(
        format=FORMAT,
        channels=CHANNELS,
        rate=SAMPLE_RATE,
        input=True,
        frames_per_buffer=FRAMES_PER_BUFFER,
    )

    try:
        while True:
            data = stream.read(FRAMES_PER_BUFFER)
            audio_data.write(data)
            data_encoded = base64.b64encode(data).decode("utf-8")
            json_data = json.dumps({"type": "audio_chunk", "data": {"chunk": str(data_encoded)}})
            try:
                await socket.send(json_data)
                await asyncio.sleep(0.1)
            except ConnectionClosedOK:
                break
            except Exception as e:
                print(f"Error sending audio: {e}")
                break
    finally:
        stream.stop_stream()
        stream.close()

async def upload_audio_to_supabase(audio_data: io.BytesIO):
    """Uploads the audio data to Supabase Storage in WAV format."""
    audio_data.seek(0)
    wav_buffer = io.BytesIO()
    with wave.open(wav_buffer, 'wb') as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(P.get_sample_size(FORMAT))
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(audio_data.read())
    wav_buffer.seek(0)

    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"audio-{timestamp}.wav"

    try:
        print(f"Uploading audio to Supabase Storage: {filename}")
        upload_response = supabase_client.storage.from_(SUPABASE_BUCKET_NAME).upload(
            path=filename,
            file=wav_buffer.getvalue(),
            file_options={"content-type": "audio/wav"}
        )
        if upload_response.path:
            print(f"Audio uploaded successfully to Supabase Storage: {upload_response.path}")
            return upload_response.path
        else:
            print(f"Erreur lors de l'upload audio vers Supabase Storage: {upload_response}")
            return None
    except Exception as e:
        print(f"Erreur lors de l'upload vers Supabase Storage: {e}")
        raise

@app.websocket("/ws/transcire")
async def websocket_transcire(websocket: WebSocket):
    audio_data = io.BytesIO()
    transcription_list = []
    supabase_path = None
    upload_success = False
    db_insert_success = False

    try:
        await websocket.accept()
        
        # Attendre de recevoir l'ID de l'utilisateur dans le premier message
        first_message = await websocket.receive_text()
        try:
            user_data = json.loads(first_message)
            user_id = user_data.get('user_id')
            if not user_id:
                raise ValueError("User ID not provided")
        except Exception as e:
            print(f"Erreur lors de la récupération de l'user_id: {e}")
            await websocket.close()
            return

        response = init_live_session(STREAMING_CONFIGURATION)
        async with connect(response["url"]) as gladia_ws:
            print("\n################ Begin session ################\n")

            send_audio_task = asyncio.create_task(send_audio(gladia_ws, audio_data))
            print_messages_task = asyncio.create_task(print_messages_from_socket(gladia_ws, websocket, transcription_list))

            done, pending = await asyncio.wait(
                [send_audio_task, print_messages_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            print("Tâches annulées.")

        print("Enregistrement terminé, uploading to Supabase Storage...")
        try:
            supabase_path = await upload_audio_to_supabase(audio_data)
            if supabase_path:
                upload_success = True
                # Passer l'user_id à save_transcription_to_database
                db_insert_success = await save_transcription_to_database(supabase_path, transcription_list, user_id)
                
                if db_insert_success:
                    try:
                        await websocket.send_text("Audio et transcription enregistrés avec succès.")
                    except RuntimeError:
                        print("Websocket déjà fermé, impossible d'envoyer le message de succès")
                else:
                    print("Échec de l'enregistrement dans la base de données")
            else:
                print("Échec de l'upload audio")
                
        except Exception as e:
            print(f"Erreur lors du processus d'enregistrement: {e}")

    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"Erreur dans websocket_transcire: {e}")
    finally:
        P.terminate()
        try:
            await websocket.close()
        except RuntimeError:
            pass

async def save_transcription_to_database(supabase_path: str, transcription_list: list, user_id: str):
    """Saves the transcription to the Supabase database."""
    try:
        audio_url = supabase_client.storage.from_(SUPABASE_BUCKET_NAME).get_public_url(supabase_path)

        data = {
            "user_id": user_id,  # UUID de l'utilisateur
            "audio_url": audio_url,
            "transcription_text": "\n".join(transcription_list),
            "created_at": datetime.datetime.now().isoformat(),
            "title": f"Audio Recording {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        }

        response = supabase_client.table(SUPABASE_TABLE_NAME).insert(data).execute()

        if response.error:
            print(f"Erreur lors de l'enregistrement dans la base de données: {response.error}")
            return False
        else:
            print("Transcription enregistrée avec succès dans la base de données.")
            print(f"URL de l'audio: {audio_url}")
            return True

    except Exception as e:
        print(f"Erreur lors de l'enregistrement dans la base de données: {e}")
        return False

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)