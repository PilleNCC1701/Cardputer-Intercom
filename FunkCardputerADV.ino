#include <M5Cardputer.h>
#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <time.h>
#include <SD.h>
#include <SPI.h>
#include <vector>
#include <algorithm>
#include <string.h>

void drawMainScreen();

// ============================================================================
// STATION 1: WLAN- & NTP-ZEITSYNCHRONISATION & FESTER ESP-NOW-KANAL
// Holt beim Start die aktuelle Uhrzeit per NTP ueber das 2,4-GHz-WLAN,
// stellt Europe/Berlin ein und fixiert danach den ESP-NOW-Kanal.
// ============================================================================
static const char* WIFI_SSID        = "network name";
static const char* WIFI_PASS        = "network password";
static const char* NTP_SERVER_1     = "pool.ntp.org";
static const char* NTP_SERVER_2     = "de.pool.ntp.org";
static const char* NTP_SERVER_3     = "time.nist.gov";
static const char* TZ_EUROPE_BERLIN = "CET-1CEST,M3.5.0,M10.5.0/3";

static constexpr uint8_t ESPNOW_CHANNEL = 1;

// ============================================================================
// STATION 2: RAEUME & GERAETEPROFILE
// Legt fest, in welchem Raum dieses Geraet steht und welches Ziel aktuell
// fuer Chat, Klingel, Plan und Sprachfunk ausgewaehlt ist.
// ============================================================================
enum RoomId : uint8_t {
  ROOM_ALL = 0,
  ROOM_EG,       // Erdgeschoss
  ROOM_OG_LENA,  // Obergeschoss Lena
  ROOM_OG_LARS,  // Obergeschoss Lars
  ROOM_DG,       // Dachgeschoss
  ROOM_COUNT
};

static const char* ROOM_NAMES[ROOM_COUNT] = {
  "Alle",
  "EG",
  "OG Lena",
  "OG Lars",
  "DG"
};

static RoomId myRoom        = ROOM_DG;
static RoomId currentTarget = ROOM_ALL;
static bool   isPlanAuthor  = true;

// ============================================================================
// STATION 3: AUDIO-STREAMING (16-BIT PCM) & SD-KARTEN-VERWALTUNG
// Schreibt Aufnahmen und Empfangsdaten blockweise auf die SD-Karte, damit
// auch laengere Sprachnachrichten ohne knappes internes RAM verarbeitet werden.
// ============================================================================
static constexpr uint32_t SAMPLE_RATE        = 16000;
static constexpr size_t   MIC_CHUNK_SAMPLES  = 512;   // Blockgroesse beim Lesen/Abspielen
static constexpr size_t   PCM_PKT_SAMPLES    = 90;    // 90 * 2 = 180 Bytes Nutzdaten + Header <= 250 Bytes
static constexpr uint32_t MAX_MSG_SECONDS    = 20;    // Sicherheitslimit pro Einzelaufnahme
static constexpr uint64_t MAX_MSG_SAMPLES    = (uint64_t)SAMPLE_RATE * MAX_MSG_SECONDS;

static constexpr uint64_t MAX_HISTORY_BYTES  = 6ULL * 1024ULL * 1024ULL * 1024ULL;
static const char* CHAT_LOG_PATH = "/history_chat.txt";
static const char* FUNK_LOG_PATH = "/history_funk.txt";
static const char* AUDIO_DIR     = "/voice_pcm";
static const char* TX_TEMP_PATH  = "/voice_pcm/tx_current.raw";
static const char* RX_TEMP_PATH  = "/voice_pcm/rx_current.raw";

static int16_t ioChunkBuf[MIC_CHUNK_SAMPLES];
static int16_t playChunkBuf[MIC_CHUNK_SAMPLES];

static uint64_t txRecordedSamples = 0;
static File     txStreamFile;
static int      micFailStreak     = 0;

static bool sdReady = false;

struct FunkRecord {
  String label;
  String filePath;
};

// ============================================================================
// STATION 4: ZUSTAENDE FUER UHR, WECKER, PLAN, VERLAUF, HELLIGKEIT & FENSTER
// Verwaltet die aktuelle Benutzeroberflaeche (Hauptbildschirm, Verlaeufe,
// Detailfenster mit ESC-Rueckkehr, Wecker, Helligkeit und Eingabefelder).
// ============================================================================
int  simHour   = 12;
int  simMinute = 0;
bool ntpSynced = false;
unsigned long lastClockTick = 0;

int  alarmHour    = 6;
int  alarmMinute  = 0;
bool alarmEnabled = true;
bool alarmRinging = false;
unsigned long lastAlarmTone = 0;

static constexpr int BRIGHTNESS_LEVELS[4] = { 10, 30, 50, 70 };
static int brightnessIdx = 2; // Start bei 50 % (max. 70 %, nie 100 %)
static constexpr int MAX_BRIGHTNESS_RAW = 255;

enum ViewMode : uint8_t {
  VIEW_HOME = 0,
  VIEW_CHAT_HISTORY,
  VIEW_FUNK_HISTORY,
  VIEW_CHAT_DETAIL,
  VIEW_FUNK_DETAIL,
  VIEW_PLAN_WINDOW,
  VIEW_CHAT_INPUT,
  VIEW_PLAN_EDIT,
  VIEW_ALARM_EDIT
};

static ViewMode viewMode = VIEW_HOME;
static String systemStatus = "Bereit";

static std::vector<String>     chatHistory;
static std::vector<FunkRecord> funkHistory;

static int selectedChatIdx = 0;
static int selectedFunkIdx = 0;
static int detailIndex     = 0;

static String roomPlans[ROOM_COUNT] = { "", "", "", "", "" };
static String textInput = "";
static RoomId planEditTarget = ROOM_OG_LENA;

// ============================================================================
// STATION 5: ESP-NOW PAKETFORMATE & ENTKOPPELTER EMPFANGSPUFFER (RINGPUFFER)
// onEspNowRecv legt eingehende Audio-Pakete nur in rxQueue ab. Das Schreiben
// auf die SD-Karte passiert danach sicher im Hauptprogramm (loop()).
// ============================================================================
static const uint8_t BROADCAST_MAC[6] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

enum PktType : uint8_t {
  PKT_CHAT      = 1,
  PKT_RING      = 2,
  PKT_PLAN      = 3,
  PKT_PCM_CHUNK = 4
};

struct __attribute__((packed)) TextPacket {
  uint8_t type;
  uint8_t target;
  char    sender[16];
  char    text[120];
};

struct __attribute__((packed)) PcmChunkPacket {
  uint8_t  type;
  uint8_t  target;
  uint16_t streamId;
  uint16_t seq;
  uint16_t total;
  uint16_t sampleCount;
  char     sender[12];
  int16_t  samples[PCM_PKT_SAMPLES];
};

static constexpr size_t RX_QUEUE_SIZE = 128;
static PcmChunkPacket rxQueue[RX_QUEUE_SIZE];
static volatile size_t rxQHead = 0;
static volatile size_t rxQTail = 0;

static File          rxStreamFile;
static uint16_t      rxActiveStreamId  = 0;
static uint16_t      rxExpectedTotal   = 0;
static uint16_t      rxReceivedCount   = 0;
static uint64_t      rxMaxSampleOffset = 0;
static char          rxActiveSender[12] = "";
static bool          rxReceivingActive = false;
static unsigned long rxLastPacketMs    = 0;
static std::vector<uint8_t> rxChunkSeen;

static uint16_t nextTxStreamId = 1;

volatile bool flagUiDirty   = true;
volatile bool flagPlayRxPcm = false;
volatile bool flagRingTone  = false;

static bool isRecordingVoice = false;
static bool startedWithG0    = false;

// Entprellung gegen Mehrfachausloesung durch Tastatur-Scanner
static unsigned long lastNavActionMs  = 0;
static unsigned long lastTypeCharMs   = 0;
static unsigned long lastEnterSendMs  = 0;
static char          lastTypedChar    = 0;
static unsigned long lastSameCharMs   = 0;
static constexpr unsigned long NAV_COOLDOWN_MS    = 220;
static constexpr unsigned long TYPE_COOLDOWN_MS   = 110;
static constexpr unsigned long SAME_CHAR_GUARD_MS = 240;
static constexpr unsigned long ENTER_COOLDOWN_MS  = 260;

bool canRunNavAction() {
  unsigned long now = millis();
  if (now - lastNavActionMs >= NAV_COOLDOWN_MS) {
    lastNavActionMs = now;
    return true;
  }
  return false;
}

bool canAcceptTypedChar(char c) {
  unsigned long now = millis();
  if (now - lastTypeCharMs < TYPE_COOLDOWN_MS) {
    return false;
  }
  if (c == lastTypedChar && (now - lastSameCharMs < SAME_CHAR_GUARD_MS)) {
    return false;
  }
  lastTypeCharMs = now;
  lastTypedChar = c;
  lastSameCharMs = now;
  return true;
}

bool canRunDeleteStep() {
  unsigned long now = millis();
  if (now - lastTypeCharMs >= 140) {
    lastTypeCharMs = now;
    return true;
  }
  return false;
}

bool canSendEnter() {
  unsigned long now = millis();
  if (now - lastEnterSendMs >= ENTER_COOLDOWN_MS) {
    lastEnterSendMs = now;
    return true;
  }
  return false;
}

void setFixedEspNowChannel(uint8_t ch) {
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(false);
  esp_wifi_set_promiscuous(true);
  esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);
  esp_wifi_set_promiscuous(false);
}

void applyBrightnessLevel() {
  int pct = BRIGHTNESS_LEVELS[brightnessIdx];
  uint8_t raw = (uint8_t)((pct * MAX_BRIGHTNESS_RAW) / 100);
  M5Cardputer.Display.setBrightness(raw);
  systemStatus = String("Helligkeit: ") + String(pct) + "%";
  flagUiDirty = true;
}

// ============================================================================
// STATION 6: ZEIT-, NTP- UND AKKU-FUNKTIONEN
// Aktualisiert die Uhrzeit laufend ueber die Systemzeit (NTP) oder bei Bedarf
// im Minuten-Fallback und liest den Ladezustand des Akkus aus.
// ============================================================================
String formatClock(int h, int m) {
  char buf[8];
  snprintf(buf, sizeof(buf), "%02d:%02d", h % 24, m % 60);
  return String(buf);
}

bool readValidLocalTime(int& outH, int& outM) {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo, 25)) {
    return false;
  }
  if (timeinfo.tm_year < (2024 - 1900)) {
    return false;
  }
  outH = timeinfo.tm_hour;
  outM = timeinfo.tm_min;
  return true;
}

void syncTimeFromNtp() {
  ntpSynced = false;
  WiFi.mode(WIFI_STA);

  if (strlen(WIFI_SSID) > 0 && strcmp(WIFI_SSID, "DEIN_WLAN_NAME") != 0) {
    WiFi.begin(WIFI_SSID, WIFI_PASS);
    unsigned long startMs = millis();
    while (WiFi.status() != WL_CONNECTED && (millis() - startMs < 7000UL)) {
      M5Cardputer.update();
      delay(120);
    }

    if (WiFi.status() == WL_CONNECTED) {
      configTzTime(TZ_EUROPE_BERLIN, NTP_SERVER_1, NTP_SERVER_2, NTP_SERVER_3);
      unsigned long ntpStart = millis();
      while (millis() - ntpStart < 5000UL) {
        int h = 0, m = 0;
        if (readValidLocalTime(h, m)) {
          simHour = h;
          simMinute = m;
          ntpSynced = true;
          break;
        }
        delay(200);
      }
    }
  }

  setFixedEspNowChannel(ESPNOW_CHANNEL);
}

void updateClock() {
  int h = 0, m = 0;
  if (readValidLocalTime(h, m)) {
    ntpSynced = true;
    if (h != simHour || m != simMinute) {
      simHour = h;
      simMinute = m;
      if (alarmEnabled && simHour == alarmHour && simMinute == alarmMinute) {
        alarmRinging = true;
      }
      if (!isRecordingVoice) flagUiDirty = true;
    }
    return;
  }

  if (millis() - lastClockTick >= 60000UL) {
    lastClockTick = millis();
    simMinute++;
    if (simMinute >= 60) {
      simMinute = 0;
      simHour = (simHour + 1) % 24;
    }
    if (alarmEnabled && simHour == alarmHour && simMinute == alarmMinute) {
      alarmRinging = true;
    }
    if (!isRecordingVoice) flagUiDirty = true;
  }
}

int getBatteryPercentSafe() {
  int batt = M5Cardputer.Power.getBatteryLevel();
  if (batt < 0) batt = 0;
  if (batt > 100) batt = 100;
  return batt;
}

// ============================================================================
// STATION 7: SD-VERLAUF & SPEICHERBUDGET (MAX. 6 GB)
// Speichert Chat- und Funk-Protokolle sowie Sprachdateien auf der SD-Karte
// und loescht bei Bedarf die aeltesten Audio-Dateien, um das 6-GB-Limit zu halten.
// ============================================================================
uint64_t folderSizeBytes(File dir) {
  uint64_t total = 0;
  while (true) {
    File entry = dir.openNextFile();
    if (!entry) break;
    if (!entry.isDirectory()) {
      total += entry.size();
    }
    entry.close();
  }
  return total;
}

void enforceSdBudget6GB() {
  if (!sdReady || isRecordingVoice) return;

  uint64_t cardTotal = SD.cardSize();
  uint64_t budget = MAX_HISTORY_BYTES;
  if (cardTotal > 0 && budget > (cardTotal * 9ULL) / 10ULL) {
    budget = (cardTotal * 9ULL) / 10ULL;
  }

  File audioDir = SD.open(AUDIO_DIR);
  uint64_t used = audioDir ? folderSizeBytes(audioDir) : 0;
  if (audioDir) audioDir.close();

  if (SD.exists(CHAT_LOG_PATH)) {
    File f = SD.open(CHAT_LOG_PATH, FILE_READ);
    if (f) { used += f.size(); f.close(); }
  }
  if (SD.exists(FUNK_LOG_PATH)) {
    File f = SD.open(FUNK_LOG_PATH, FILE_READ);
    if (f) { used += f.size(); f.close(); }
  }

  while (used > budget && sdReady) {
    File dir = SD.open(AUDIO_DIR);
    if (!dir) break;
    String oldestName = "";
    uint64_t oldestSize = 0;
    while (true) {
      File entry = dir.openNextFile();
      if (!entry) break;
      String ename = String(entry.name());
      int slashIdx = ename.lastIndexOf('/');
      if (slashIdx >= 0) ename = ename.substring(slashIdx + 1);
      if (!entry.isDirectory() && ename != "tx_current.raw" && ename != "rx_current.raw") {
        oldestName = String(AUDIO_DIR) + "/" + ename;
        oldestSize = entry.size();
        entry.close();
        break;
      }
      entry.close();
    }
    dir.close();

    if (oldestName.length() > 0 && SD.remove(oldestName.c_str())) {
      used = (used > oldestSize) ? (used - oldestSize) : 0;
    } else {
      break;
    }
  }
}

void appendLogLine(const char* path, const String& line) {
  if (!sdReady) return;
  File f = SD.open(path, FILE_APPEND);
  if (f) {
    f.printf("[%s] %s\n", formatClock(simHour, simMinute).c_str(), line.c_str());
    f.close();
  }
}

void addChatEntry(const String& sender, const String& msg) {
  String entry = "[" + formatClock(simHour, simMinute) + "] " + sender + ": " + msg;
  chatHistory.push_back(entry);
  if (chatHistory.size() > 30) {
    chatHistory.erase(chatHistory.begin());
  }
  selectedChatIdx = (int)chatHistory.size() - 1;
  appendLogLine(CHAT_LOG_PATH, entry);
}

void addFunkEntry(const String& info, const char* srcFilePath, uint64_t sampleCount) {
  uint64_t bytesLen = sampleCount * sizeof(int16_t);
  FunkRecord rec;
  rec.label = "[" + formatClock(simHour, simMinute) + "] " + info;
  if (bytesLen > 0) {
    rec.label += " (" + String((unsigned)(bytesLen / 1024)) + "KB)";
  }

  if (sdReady && srcFilePath != nullptr && SD.exists(srcFilePath) && bytesLen > 0) {
    char fname[48];
    snprintf(fname, sizeof(fname), "%s/f_%02d%02d_%lu.raw",
             AUDIO_DIR, simHour, simMinute, millis() % 100000UL);
    File src = SD.open(srcFilePath, FILE_READ);
    File dst = SD.open(fname, FILE_WRITE);
    if (src && dst) {
      while (src.available()) {
        size_t n = src.read(reinterpret_cast<uint8_t*>(playChunkBuf), sizeof(playChunkBuf));
        if (n == 0) break;
        dst.write(reinterpret_cast<const uint8_t*>(playChunkBuf), n);
      }
      dst.flush();
      rec.filePath = String(fname);
    }
    if (src) src.close();
    if (dst) dst.close();
    enforceSdBudget6GB();
  }

  funkHistory.push_back(rec);
  if (funkHistory.size() > 30) {
    funkHistory.erase(funkHistory.begin());
  }
  selectedFunkIdx = (int)funkHistory.size() - 1;
  appendLogLine(FUNK_LOG_PATH, rec.label);
}

// ============================================================================
// STATION 8: LAUTSPRECHER-WIEDERGABE VON PCM-DATEIEN & KLINGELTON
// Schaltet das Mikrofon ab, aktiviert den Lautsprecher und spielt die gespeicherte
// 16-Bit-PCM-Datei von der SD-Karte in Bloecken ab.
// ============================================================================
void playPcmFile(const String& path, const char* statusLabel) {
  if (!sdReady || path.length() == 0 || !SD.exists(path.c_str())) return;

  File f = SD.open(path.c_str(), FILE_READ);
  if (!f) return;

  M5Cardputer.Mic.end();
  delay(25);
  M5Cardputer.Speaker.begin();
  M5Cardputer.Speaker.setVolume(255);

  String prevStatus = systemStatus;
  systemStatus = statusLabel;
  drawMainScreen();

  while (f.available()) {
    M5Cardputer.update();
    size_t bytesRead = f.read(reinterpret_cast<uint8_t*>(playChunkBuf), sizeof(playChunkBuf));
    size_t samplesRead = bytesRead / sizeof(int16_t);
    if (samplesRead == 0) break;

    M5Cardputer.Speaker.playRaw(playChunkBuf, samplesRead, SAMPLE_RATE, false, 1, 0);
    while (M5Cardputer.Speaker.isPlaying()) {
      M5Cardputer.update();
      delay(4);
    }
  }
  f.close();

  M5Cardputer.Speaker.end();
  delay(25);
  systemStatus = prevStatus;
  flagUiDirty = true;
}

void playLoudRingTone() {
  M5Cardputer.Mic.end();
  delay(20);
  M5Cardputer.Speaker.begin();
  M5Cardputer.Speaker.setVolume(255);

  String prevStatus = systemStatus;
  systemStatus = "KLINGEL!";
  drawMainScreen();

  for (int rep = 0; rep < 3; rep++) {
    M5Cardputer.Speaker.tone(1900, 140);
    while (M5Cardputer.Speaker.isPlaying()) {
      M5Cardputer.update();
      delay(4);
    }
    delay(45);
    M5Cardputer.Speaker.tone(2600, 180);
    while (M5Cardputer.Speaker.isPlaying()) {
      M5Cardputer.update();
      delay(4);
    }
    delay(120);
  }

  M5Cardputer.Speaker.end();
  delay(20);
  systemStatus = prevStatus;
  flagUiDirty = true;
}

// ============================================================================
// STATION 9: ESP-NOW VERSAND & EMPFANG (CHAT, KLINGEL, PLAN & AUDIO-PAKETE)
// Sendet Steuer-/Textpakete sowie PCM-Audiochunks und nimmt eingehende Pakete
// entgegen.
// ============================================================================
void ensureBroadcastPeer() {
  if (esp_now_is_peer_exist(BROADCAST_MAC)) return;
  esp_now_peer_info_t peer;
  memset(&peer, 0, sizeof(peer));
  memcpy(peer.peer_addr, BROADCAST_MAC, 6);
  peer.channel = ESPNOW_CHANNEL;
  peer.encrypt = false;
  esp_now_add_peer(&peer);
}

void sendTextPacket(PktType type, RoomId target, const String& text) {
  ensureBroadcastPeer();
  TextPacket pkt;
  memset(&pkt, 0, sizeof(pkt));
  pkt.type = (uint8_t)type;
  pkt.target = (uint8_t)target;
  strncpy(pkt.sender, ROOM_NAMES[myRoom], sizeof(pkt.sender) - 1);
  strncpy(pkt.text, text.c_str(), sizeof(pkt.text) - 1);

  esp_now_send(BROADCAST_MAC, reinterpret_cast<const uint8_t*>(&pkt), sizeof(pkt));
}

void sendPcmStreamFromSd() {
  if (!sdReady || txRecordedSamples == 0 || !SD.exists(TX_TEMP_PATH)) {
    systemStatus = "Kein Audio";
    flagUiDirty = true;
    return;
  }

  File f = SD.open(TX_TEMP_PATH, FILE_READ);
  if (!f) {
    systemStatus = "SD Lesefehler";
    flagUiDirty = true;
    return;
  }

  uint64_t totalBytes = f.size();
  uint64_t totalSamples = totalBytes / sizeof(int16_t);
  if (totalSamples == 0) {
    f.close();
    systemStatus = "Audio leer";
    flagUiDirty = true;
    return;
  }

  ensureBroadcastPeer();
  systemStatus = "Sende PCM...";
  drawMainScreen();

  uint16_t streamId = nextTxStreamId++;
  if (nextTxStreamId == 0) nextTxStreamId = 1;

  uint16_t totalChunks = (uint16_t)((totalSamples + PCM_PKT_SAMPLES - 1) / PCM_PKT_SAMPLES);
  uint16_t okCount = 0;

  for (uint16_t seq = 0; seq < totalChunks; seq++) {
    size_t readBytes = f.read(reinterpret_cast<uint8_t*>(playChunkBuf), PCM_PKT_SAMPLES * sizeof(int16_t));
    size_t nSamples = readBytes / sizeof(int16_t);
    if (nSamples == 0) break;

    PcmChunkPacket pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.type = PKT_PCM_CHUNK;
    pkt.target = (uint8_t)currentTarget;
    pkt.streamId = streamId;
    pkt.seq = seq;
    pkt.total = totalChunks;
    pkt.sampleCount = (uint16_t)nSamples;
    strncpy(pkt.sender, ROOM_NAMES[myRoom], sizeof(pkt.sender) - 1);
    memcpy(pkt.samples, playChunkBuf, nSamples * sizeof(int16_t));

    size_t headerBytes = offsetof(PcmChunkPacket, samples);
    size_t wireSize = headerBytes + nSamples * sizeof(int16_t);

    bool sentThisChunk = false;
    for (int retry = 0; retry < 8; retry++) {
      esp_err_t err = esp_now_send(BROADCAST_MAC, reinterpret_cast<const uint8_t*>(&pkt), wireSize);
      if (err == ESP_OK) {
        okCount++;
        sentThisChunk = true;
        break;
      }
      delay(5);
      yield();
    }
    if (!sentThisChunk) {
      delay(6);
    }

    delay(12);

    if (seq % 6 == 0) {
      M5Cardputer.update();
      yield();
    }
  }
  f.close();

  String summary = String("-> ") + ROOM_NAMES[currentTarget] + " (" + String((unsigned)okCount) + "/" + String((unsigned)totalChunks) + ")";
  addFunkEntry(summary, TX_TEMP_PATH, totalSamples);
  systemStatus = "PCM gesendet";
  flagUiDirty = true;
}

void onEspNowRecv(const esp_now_recv_info* info, const uint8_t* data, int len) {
  (void)info;
  if (len <= 0 || data == nullptr) return;
  uint8_t ptype = data[0];

  if (ptype == PKT_CHAT && len >= (int)sizeof(TextPacket)) {
    TextPacket pkt;
    memcpy(&pkt, data, sizeof(pkt));
    if (pkt.target != ROOM_ALL && pkt.target != (uint8_t)myRoom) return;
    pkt.sender[sizeof(pkt.sender) - 1] = '\0';
    pkt.text[sizeof(pkt.text) - 1] = '\0';
    addChatEntry(String(pkt.sender), String(pkt.text));
    systemStatus = String("Chat: ") + pkt.sender;
    if (!isRecordingVoice) flagUiDirty = true;
  } else if (ptype == PKT_RING && len >= (int)sizeof(TextPacket)) {
    TextPacket pkt;
    memcpy(&pkt, data, sizeof(pkt));
    if (pkt.target != ROOM_ALL && pkt.target != (uint8_t)myRoom) return;
    pkt.sender[sizeof(pkt.sender) - 1] = '\0';
    systemStatus = String("Klingel ") + pkt.sender;
    flagRingTone = true;
    if (!isRecordingVoice) flagUiDirty = true;
  } else if (ptype == PKT_PLAN && len >= (int)sizeof(TextPacket)) {
    TextPacket pkt;
    memcpy(&pkt, data, sizeof(pkt));
    if (pkt.target != ROOM_ALL && pkt.target != (uint8_t)myRoom) return;
    pkt.text[sizeof(pkt.text) - 1] = '\0';
    if (pkt.target < ROOM_COUNT) {
      roomPlans[pkt.target] = String(pkt.text);
    }
    if (myRoom != ROOM_ALL) {
      roomPlans[myRoom] = String(pkt.text);
    }
    systemStatus = "Neuer Plan [P]";
    if (!isRecordingVoice) flagUiDirty = true;
  } else if (ptype == PKT_PCM_CHUNK) {
    size_t headerBytes = offsetof(PcmChunkPacket, samples);
    if (len < (int)(headerBytes + sizeof(int16_t))) return;

    PcmChunkPacket pkt;
    memset(&pkt, 0, sizeof(pkt));
    memcpy(&pkt, data, std::min((size_t)len, sizeof(pkt)));

    if (pkt.target != ROOM_ALL && pkt.target != (uint8_t)myRoom) return;
    if (pkt.total == 0 || pkt.seq >= pkt.total) return;
    if (pkt.sampleCount == 0 || pkt.sampleCount > PCM_PKT_SAMPLES) return;

    size_t nextHead = (rxQHead + 1) % RX_QUEUE_SIZE;
    if (nextHead != rxQTail) {
      rxQueue[rxQHead] = pkt;
      rxQHead = nextHead;
    }
  }
}

void finalizeRxStream() {
  if (!rxReceivingActive) return;
  if (rxStreamFile) {
    rxStreamFile.flush();
    rxStreamFile.close();
  }
  rxReceivingActive = false;

  if (rxMaxSampleOffset > 0 && rxReceivedCount > 0) {
    addFunkEntry(String("Von ") + rxActiveSender, RX_TEMP_PATH, rxMaxSampleOffset);
    systemStatus = String("Funk von ") + rxActiveSender;
    flagPlayRxPcm = true;
    flagUiDirty = true;
  }
}

void processRxQueueToSd() {
  if (!sdReady || isRecordingVoice) return;

  if (rxReceivingActive && rxReceivedCount > 0 && (millis() - rxLastPacketMs > 650UL)) {
    finalizeRxStream();
  }

  // Wiedergabe erst starten, wenn Stream abgeschlossen und Ringpuffer leer ist
  if (flagPlayRxPcm && (rxReceivingActive || rxQTail != rxQHead)) {
    return;
  }

  int processed = 0;
  while (rxQTail != rxQHead && processed < 16) {
    PcmChunkPacket pkt = rxQueue[rxQTail];
    rxQTail = (rxQTail + 1) % RX_QUEUE_SIZE;
    processed++;

    bool newStream = !rxReceivingActive ||
                     (pkt.streamId != rxActiveStreamId) ||
                     (pkt.total != rxExpectedTotal);

    if (newStream) {
      if (rxReceivingActive) {
        finalizeRxStream();
      }
      if (rxStreamFile) rxStreamFile.close();
      SD.remove(RX_TEMP_PATH);
      rxStreamFile = SD.open(RX_TEMP_PATH, FILE_WRITE);
      rxActiveStreamId = pkt.streamId;
      rxExpectedTotal = pkt.total;
      rxReceivedCount = 0;
      rxMaxSampleOffset = 0;
      rxChunkSeen.assign(pkt.total, 0);
      strncpy(rxActiveSender, pkt.sender, sizeof(rxActiveSender) - 1);
      rxActiveSender[sizeof(rxActiveSender) - 1] = '\0';
      rxReceivingActive = (rxStreamFile != false);
    }

    if (rxReceivingActive && rxStreamFile && pkt.seq < rxExpectedTotal) {
      uint64_t byteOffset = (uint64_t)pkt.seq * PCM_PKT_SAMPLES * sizeof(int16_t);
      rxStreamFile.seek(byteOffset);
      rxStreamFile.write(reinterpret_cast<const uint8_t*>(pkt.samples), pkt.sampleCount * sizeof(int16_t));
      rxLastPacketMs = millis();

      if (!rxChunkSeen[pkt.seq]) {
        rxChunkSeen[pkt.seq] = 1;
        rxReceivedCount++;
      }

      uint64_t endSample = (uint64_t)pkt.seq * PCM_PKT_SAMPLES + pkt.sampleCount;
      if (endSample > rxMaxSampleOffset) {
        rxMaxSampleOffset = endSample;
      }

      if (rxReceivedCount >= rxExpectedTotal) {
        finalizeRxStream();
        break;
      }
    }
  }
}

// ============================================================================
// STATION 10: BILDSCHIRMZEICHNUNG (HAUPTBILD, VERLAEUFE, DETAILFENSTER & PLAN)
// ============================================================================
void drawBatteryIcon(int x, int y, int pct) {
  auto& d = M5Cardputer.Display;
  int bodyW = 20;
  int bodyH = 10;

  d.drawRect(x, y, bodyW, bodyH, TFT_WHITE);
  d.fillRect(x + bodyW, y + 3, 2, 4, TFT_WHITE);

  uint16_t fillCol = TFT_GREEN;
  if (pct <= 20) fillCol = TFT_RED;
  else if (pct <= 55) fillCol = TFT_YELLOW;

  int maxInnerW = bodyW - 4;
  int innerW = (pct * maxInnerW) / 100;
  if (pct > 0 && innerW < 2) innerW = 2;
  if (innerW > 0) {
    d.fillRect(x + 2, y + 2, innerW, bodyH - 4, fillCol);
  }
}

void drawSoftkeyTile(int idx, int keyNum, const char* label, uint16_t accent, bool active) {
  auto& d = M5Cardputer.Display;
  int w = 46;
  int h = 26;
  int x = 5 + idx * 47;
  int y = 106;

  uint16_t bg = active ? accent : TFT_DARKGREY;
  uint16_t fg = active ? TFT_BLACK : TFT_WHITE;

  d.fillRoundRect(x, y, w, h, 4, bg);
  d.drawRoundRect(x, y, w, h, 4, accent);
  d.setTextSize(1);
  d.setTextColor(fg, bg);
  d.setCursor(x + 18, y + 3);
  d.printf("%d", keyNum);
  d.setCursor(x + 4, y + 14);
  d.println(label);
}

void drawPlanWindow() {
  auto& d = M5Cardputer.Display;
  d.fillScreen(TFT_BLACK);
  d.drawRect(0, 0, 240, 135, TFT_GREEN);
  d.setTextSize(1);

  d.setTextColor(TFT_GREEN, TFT_BLACK);
  d.setCursor(6, 5);
  d.printf("Plan (%s):", ROOM_NAMES[myRoom]);

  d.setTextColor(TFT_GREEN, TFT_BLACK);
  d.drawRoundRect(154, 3, 80, 13, 3, TFT_GREEN);
  d.setCursor(159, 6);
  d.println("Zurueck [esc]");

  d.drawFastHLine(1, 20, 238, TFT_DARKGREY);

  String activePlan = roomPlans[myRoom];
  if (activePlan.length() == 0) {
    d.setTextColor(TFT_LIGHTGREY, TFT_BLACK);
    d.setCursor(10, 56);
    d.println("leer");
  } else {
    d.setTextColor(TFT_WHITE, TFT_BLACK);
    d.setCursor(10, 48);
    if (activePlan.length() > 34) {
      d.println(activePlan.substring(0, 34));
      d.setCursor(10, 62);
      d.println(activePlan.substring(34, std::min((int)activePlan.length(), 68)));
    } else {
      d.println(activePlan);
    }
  }

  d.setTextColor(TFT_LIGHTGREY, TFT_BLACK);
  d.setCursor(8, 118);
  d.println("ESC / ` / b: Zurueck");
  flagUiDirty = false;
}

void drawChatDetailWindow() {
  auto& d = M5Cardputer.Display;
  d.fillScreen(TFT_BLACK);
  d.drawRect(0, 0, 240, 135, TFT_ORANGE);
  d.setTextSize(1);

  d.setTextColor(TFT_ORANGE, TFT_BLACK);
  d.setCursor(6, 5);
  d.println("Chat Detail:");

  d.setTextColor(TFT_ORANGE, TFT_BLACK);
  d.drawRoundRect(154, 3, 80, 13, 3, TFT_ORANGE);
  d.setCursor(159, 6);
  d.println("Zurueck [esc]");

  d.drawFastHLine(1, 20, 238, TFT_DARKGREY);

  String msg = (detailIndex >= 0 && detailIndex < (int)chatHistory.size())
    ? chatHistory[(size_t)detailIndex]
    : "Kein Eintrag";

  d.setTextColor(TFT_WHITE, TFT_BLACK);
  int y = 30;
  for (size_t pos = 0; pos < msg.length() && y < 105; pos += 36) {
    d.setCursor(8, y);
    d.println(msg.substring(pos, std::min(msg.length(), pos + 36)));
    y += 14;
  }

  d.setTextColor(TFT_LIGHTGREY, TFT_BLACK);
  d.setCursor(8, 118);
  d.println("ESC / ` / b: Zurueck");
  flagUiDirty = false;
}

void drawFunkDetailWindow() {
  auto& d = M5Cardputer.Display;
  d.fillScreen(TFT_BLACK);
  d.drawRect(0, 0, 240, 135, TFT_ORANGE);
  d.setTextSize(1);

  d.setTextColor(TFT_ORANGE, TFT_BLACK);
  d.setCursor(6, 5);
  d.println("Funk Aufnahme:");

  d.setTextColor(TFT_ORANGE, TFT_BLACK);
  d.drawRoundRect(154, 3, 80, 13, 3, TFT_ORANGE);
  d.setCursor(159, 6);
  d.println("Zurueck [esc]");

  d.drawFastHLine(1, 20, 238, TFT_DARKGREY);

  String lbl = (detailIndex >= 0 && detailIndex < (int)funkHistory.size())
    ? funkHistory[(size_t)detailIndex].label
    : "Keine Aufnahme";

  d.setTextColor(TFT_WHITE, TFT_BLACK);
  d.setCursor(8, 34);
  if (lbl.length() > 36) {
    d.println(lbl.substring(0, 36));
    d.setCursor(8, 48);
    d.println(lbl.substring(36, std::min((int)lbl.length(), 72)));
  } else {
    d.println(lbl);
  }

  d.setTextColor(TFT_GREEN, TFT_BLACK);
  d.setCursor(8, 74);
  d.println("Enter: Erneut abspielen");

  d.setTextColor(TFT_LIGHTGREY, TFT_BLACK);
  d.setCursor(8, 118);
  d.println("ESC / ` / b: Zurueck");
  flagUiDirty = false;
}

void drawMainScreen() {
  if (viewMode == VIEW_PLAN_WINDOW) {
    drawPlanWindow();
    return;
  }
  if (viewMode == VIEW_CHAT_DETAIL) {
    drawChatDetailWindow();
    return;
  }
  if (viewMode == VIEW_FUNK_DETAIL) {
    drawFunkDetailWindow();
    return;
  }

  auto& d = M5Cardputer.Display;
  d.fillScreen(TFT_BLACK);
  d.setTextSize(1);

  d.setTextColor(TFT_WHITE, TFT_BLACK);
  d.setCursor(4, 3);
  d.printf("Ziel->%s", ROOM_NAMES[currentTarget]);

  drawBatteryIcon(112, 3, getBatteryPercentSafe());

  d.setTextColor(alarmEnabled ? TFT_GREEN : TFT_SILVER, TFT_BLACK);
  d.setCursor(164, 3);
  d.printf("W:%02d:%02d", alarmHour, alarmMinute);

  d.setTextColor(alarmRinging ? TFT_RED : TFT_YELLOW, TFT_BLACK);
  d.setCursor(4, 15);
  String stLine = alarmRinging ? "WECKER AKTIV!" : (String("Systemstatus: ") + systemStatus);
  if (stLine.length() > 26) stLine = stLine.substring(0, 26);
  d.println(stLine);

  d.setTextColor(TFT_GREEN, TFT_BLACK);
  d.drawRoundRect(168, 13, 68, 12, 3, TFT_GREEN);
  d.setCursor(173, 15);
  d.println("Plan [P]");

  bool inFunkHist = (viewMode == VIEW_FUNK_HISTORY);
  bool inChatHist = (viewMode == VIEW_CHAT_HISTORY);

  d.setTextColor(TFT_ORANGE, TFT_BLACK);
  d.drawRoundRect(4, 26, 112, 12, 3, TFT_ORANGE);
  if (inFunkHist) d.drawRect(3, 25, 114, 14, TFT_WHITE);
  d.setCursor(8, 28);
  d.println("Verlauf: Funk [F]");

  d.setTextColor(TFT_ORANGE, TFT_BLACK);
  d.drawRoundRect(168, 26, 68, 12, 3, TFT_ORANGE);
  if (inChatHist) d.drawRect(167, 25, 70, 14, TFT_WHITE);
  d.setCursor(173, 28);
  d.println("Chat [C]");

  d.drawFastHLine(0, 40, 240, TFT_DARKGREY);

  d.setTextColor(TFT_WHITE, TFT_BLACK);
  d.setCursor(4, 43);
  if (inFunkHist) {
    d.println("Funk [;/ . + Enter]:");
  } else if (inChatHist) {
    d.println("Chat [;/ . + Enter]:");
  } else {
    d.println("Aktuell:");
  }

  int maxLines = (viewMode == VIEW_CHAT_INPUT || viewMode == VIEW_PLAN_EDIT || viewMode == VIEW_ALARM_EDIT) ? 2 : 3;
  int rowY = 55;

  if (inFunkHist) {
    int count = (int)funkHistory.size();
    if (count == 0) {
      d.setTextColor(TFT_LIGHTGREY, TFT_BLACK);
      d.setCursor(4, rowY);
      d.println("1. leer");
    } else {
      if (selectedFunkIdx < 0) selectedFunkIdx = 0;
      if (selectedFunkIdx >= count) selectedFunkIdx = count - 1;
      int startIdx = std::max(0, std::min(selectedFunkIdx - 1, count - maxLines));
      for (int i = startIdx; i < count && (i - startIdx) < maxLines; i++) {
        bool isSel = (i == selectedFunkIdx);
        d.setTextColor(isSel ? TFT_ORANGE : TFT_LIGHTGREY, TFT_BLACK);
        d.setCursor(4, rowY);
        String item = String(isSel ? ">" : " ") + String((i - startIdx) + 1) + ". " + funkHistory[(size_t)i].label;
        if (item.length() > 36) item = item.substring(0, 36);
        d.println(item);
        rowY += 11;
      }
    }
  } else {
    int count = (int)chatHistory.size();
    if (count == 0) {
      d.setTextColor(TFT_LIGHTGREY, TFT_BLACK);
      d.setCursor(4, rowY);
      d.println("1. leer");
    } else {
      if (selectedChatIdx < 0) selectedChatIdx = 0;
      if (selectedChatIdx >= count) selectedChatIdx = count - 1;
      int startIdx = inChatHist
        ? std::max(0, std::min(selectedChatIdx - 1, count - maxLines))
        : std::max(0, count - maxLines);
      for (int i = startIdx; i < count && (i - startIdx) < maxLines; i++) {
        bool isSel = (inChatHist && i == selectedChatIdx);
        d.setTextColor(isSel ? TFT_ORANGE : TFT_LIGHTGREY, TFT_BLACK);
        d.setCursor(4, rowY);
        String prefix = inChatHist ? (String(isSel ? ">" : " ") + String((i - startIdx) + 1) + ". ")
                                   : (String((i - startIdx) + 1) + ". ");
        String item = prefix + chatHistory[(size_t)i];
        if (item.length() > 36) item = item.substring(0, 36);
        d.println(item);
        rowY += 11;
      }
    }
  }

  if (viewMode == VIEW_CHAT_INPUT) {
    d.fillRect(2, 88, 236, 16, TFT_NAVY);
    d.drawRect(2, 88, 236, 16, TFT_CYAN);
    d.setTextColor(TFT_WHITE, TFT_NAVY);
    d.setCursor(6, 92);
    String shown = textInput;
    if (shown.length() > 32) shown = shown.substring(shown.length() - 32);
    d.printf("Chat: %s_", shown.c_str());
  } else if (viewMode == VIEW_PLAN_EDIT) {
    d.fillRect(2, 88, 236, 16, TFT_DARKGREEN);
    d.drawRect(2, 88, 236, 16, TFT_GREEN);
    d.setTextColor(TFT_WHITE, TFT_DARKGREEN);
    d.setCursor(6, 92);
    String shown = textInput;
    if (shown.length() > 26) shown = shown.substring(shown.length() - 26);
    d.printf("Plan[%s]: %s_", ROOM_NAMES[planEditTarget], shown.c_str());
  } else if (viewMode == VIEW_ALARM_EDIT) {
    d.fillRect(2, 88, 236, 16, TFT_MAROON);
    d.drawRect(2, 88, 236, 16, TFT_YELLOW);
    d.setTextColor(TFT_WHITE, TFT_MAROON);
    d.setCursor(6, 92);
    d.printf("Wecker %02d:%02d (%s) [;/.]", alarmHour, alarmMinute, alarmEnabled ? "AN" : "AUS");
  }

  drawSoftkeyTile(0, 1, "Chat",    TFT_CYAN,   viewMode == VIEW_CHAT_INPUT);
  drawSoftkeyTile(1, 2, "Klingel", TFT_YELLOW, false);
  drawSoftkeyTile(2, 3, "Plan",    TFT_GREEN,  viewMode == VIEW_PLAN_EDIT);
  drawSoftkeyTile(3, 4, "Wecker",  TFT_ORANGE, viewMode == VIEW_ALARM_EDIT);
  drawSoftkeyTile(4, 5, "Ziel",    TFT_MAGENTA, false);

  flagUiDirty = false;
}

// ============================================================================
// STATION 11: SPRACHAUFNAHME (PUSH-TO-TALK MIT '0' ODER G0)
// ============================================================================
void startVoiceRecording(bool fromG0) {
  if (!sdReady || isRecordingVoice) return;

  if (M5Cardputer.Speaker.isPlaying()) {
    M5Cardputer.Speaker.stop();
  }
  M5Cardputer.Speaker.end();
  delay(20);
  M5Cardputer.Mic.begin();
  delay(20);

  if (SD.exists(TX_TEMP_PATH)) {
    SD.remove(TX_TEMP_PATH);
  }
  txStreamFile = SD.open(TX_TEMP_PATH, FILE_WRITE);
  if (!txStreamFile) {
    M5Cardputer.Mic.end();
    systemStatus = "SD Schreibfehler";
    flagUiDirty = true;
    return;
  }

  txRecordedSamples = 0;
  micFailStreak = 0;
  isRecordingVoice = true;
  startedWithG0 = fromG0;
  systemStatus = "AUFNAHME...";
  drawMainScreen();
}

void stopVoiceRecordingAndSend() {
  if (!isRecordingVoice) return;
  isRecordingVoice = false;

  if (txStreamFile) {
    txStreamFile.flush();
    txStreamFile.close();
  }
  M5Cardputer.Mic.end();
  delay(20);

  if (txRecordedSamples > 0) {
    sendPcmStreamFromSd();
  } else {
    systemStatus = "Aufnahme leer";
    flagUiDirty = true;
  }
}

// ============================================================================
// STATION 12: SETUP & LOOP
// ============================================================================
void setup() {
  auto cfg = M5.config();
  M5Cardputer.begin(cfg, true);

  M5Cardputer.Display.setRotation(1);
  M5Cardputer.Display.setTextSize(1);
  applyBrightnessLevel();
  systemStatus = "Starte...";
  drawMainScreen();

  SPI.begin(40, 39, 14, 12);
  delay(40);
  sdReady = SD.begin(12, SPI, 4000000);
  if (!sdReady) {
    SD.end();
    delay(40);
    SPI.begin(40, 39, 14, 12);
    sdReady = SD.begin(12, SPI, 1000000);
  }

  if (sdReady) {
    if (!SD.exists(AUDIO_DIR)) {
      SD.mkdir(AUDIO_DIR);
    }
    enforceSdBudget6GB();
  }

  syncTimeFromNtp();

  if (esp_now_init() == ESP_OK) {
    esp_now_register_recv_cb(onEspNowRecv);
    ensureBroadcastPeer();
  }

  systemStatus = sdReady ? "Bereit" : "Keine SD";
  flagUiDirty = true;
}

void loop() {
  M5Cardputer.update();
  updateClock();
  processRxQueueToSd();

  // Wecker-Signal abspielen, solange aktiv
  if (alarmRinging && !isRecordingVoice && (millis() - lastAlarmTone >= 900UL)) {
    lastAlarmTone = millis();
    M5Cardputer.Mic.end();
    M5Cardputer.Speaker.begin();
    M5Cardputer.Speaker.setVolume(255);
    M5Cardputer.Speaker.tone(2200, 220);
    while (M5Cardputer.Speaker.isPlaying()) {
      M5Cardputer.update();
      delay(4);
    }
    M5Cardputer.Speaker.end();
  }

  // Lauteres Klingeln bei eingehendem Klingel-Paket
  if (flagRingTone && !isRecordingVoice) {
    flagRingTone = false;
    playLoudRingTone();
  }

  // Eingehende Sprachnachricht sofort abspielen, sobald komplett empfangen
  if (flagPlayRxPcm && !isRecordingVoice && !rxReceivingActive && (rxQTail == rxQHead)) {
    flagPlayRxPcm = false;
    if (sdReady && SD.exists(RX_TEMP_PATH)) {
      playPcmFile(RX_TEMP_PATH, "Neue Funk-Msg");
    }
  }

  bool inTextInputMode = (viewMode == VIEW_CHAT_INPUT || viewMode == VIEW_PLAN_EDIT);
  bool pttKeyPressed = (!inTextInputMode && M5Cardputer.Keyboard.isKeyPressed('0'));
  bool g0Pressed     = (M5Cardputer.BtnA.isPressed());

  // Push-to-Talk starten / halten / beenden
  if (!isRecordingVoice) {
    if (pttKeyPressed) {
      startVoiceRecording(false);
    } else if (g0Pressed) {
      startVoiceRecording(true);
    }
  } else {
    bool keepRecording = startedWithG0 ? g0Pressed : pttKeyPressed;
    if (!keepRecording || txRecordedSamples >= MAX_MSG_SAMPLES) {
      stopVoiceRecordingAndSend();
    } else {
      if (M5Cardputer.Mic.record(ioChunkBuf, MIC_CHUNK_SAMPLES, SAMPLE_RATE)) {
        micFailStreak = 0;
        if (txStreamFile) {
          size_t written = txStreamFile.write(reinterpret_cast<const uint8_t*>(ioChunkBuf),
                                              MIC_CHUNK_SAMPLES * sizeof(int16_t));
          txRecordedSamples += (written / sizeof(int16_t));
        }
      } else {
        micFailStreak++;
        if (micFailStreak > 25) {
          stopVoiceRecordingAndSend();
        }
      }
      return;
    }
  }

  // Tastatureingaben & Navigation
  if (M5Cardputer.Keyboard.isChange() && M5Cardputer.Keyboard.isPressed()) {
    auto ks = M5Cardputer.Keyboard.keysState();

    // Wecker mit beliebiger Taste quittieren, falls er klingelt
    if (alarmRinging) {
      alarmRinging = false;
      systemStatus = "Wecker aus";
      flagUiDirty = true;
      return;
    }

    bool escPressed = M5Cardputer.Keyboard.isKeyPressed('`') ||
                      M5Cardputer.Keyboard.isKeyPressed('b') ||
                      M5Cardputer.Keyboard.isKeyPressed('B');

    // Detail- und Plan-Fenster schliessen
    if (viewMode == VIEW_PLAN_WINDOW || viewMode == VIEW_CHAT_DETAIL || viewMode == VIEW_FUNK_DETAIL) {
      if (viewMode == VIEW_FUNK_DETAIL && ks.enter && canSendEnter()) {
        if (detailIndex >= 0 && detailIndex < (int)funkHistory.size()) {
          playPcmFile(funkHistory[(size_t)detailIndex].filePath, "Wiedergabe...");
        }
        return;
      }
      if (escPressed && canRunNavAction()) {
        viewMode = (viewMode == VIEW_FUNK_DETAIL) ? VIEW_FUNK_HISTORY :
                   (viewMode == VIEW_CHAT_DETAIL) ? VIEW_CHAT_HISTORY : VIEW_HOME;
        flagUiDirty = true;
      }
      return;
    }

    // Texteingabe fuer Chat oder Plan
    if (viewMode == VIEW_CHAT_INPUT || viewMode == VIEW_PLAN_EDIT) {
      if (escPressed && canRunNavAction()) {
        textInput = "";
        viewMode = VIEW_HOME;
        systemStatus = "Abgebrochen";
        flagUiDirty = true;
        return;
      }
      if (ks.del && canRunDeleteStep()) {
        if (textInput.length() > 0) {
          textInput.remove(textInput.length() - 1);
          flagUiDirty = true;
        }
        return;
      }
      if (ks.enter && canSendEnter()) {
        if (textInput.length() > 0) {
          if (viewMode == VIEW_CHAT_INPUT) {
            sendTextPacket(PKT_CHAT, currentTarget, textInput);
            addChatEntry(String(ROOM_NAMES[myRoom]), textInput);
            systemStatus = "Chat gesendet";
          } else if (viewMode == VIEW_PLAN_EDIT) {
            sendTextPacket(PKT_PLAN, planEditTarget, textInput);
            roomPlans[planEditTarget] = textInput;
            systemStatus = "Plan gesendet";
          }
        }
        textInput = "";
        viewMode = VIEW_HOME;
        flagUiDirty = true;
        return;
      }
      for (char c : ks.word) {
        if (c >= 32 && c < 127 && textInput.length() < 90 && canAcceptTypedChar(c)) {
          textInput += c;
          flagUiDirty = true;
        }
      }
      return;
    }

    // Wecker-Einstellung
    if (viewMode == VIEW_ALARM_EDIT && canRunNavAction()) {
      if (M5Cardputer.Keyboard.isKeyPressed(';')) {
        alarmMinute = (alarmMinute + 5) % 60;
        if (alarmMinute == 0) alarmHour = (alarmHour + 1) % 24;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('.')) {
        alarmEnabled = !alarmEnabled;
        flagUiDirty = true;
        return;
      }
      if (ks.enter || escPressed) {
        viewMode = VIEW_HOME;
        systemStatus = alarmEnabled ? "Wecker aktiv" : "Wecker aus";
        flagUiDirty = true;
        return;
      }
    }

    // Navigation in Verlaeufen (Funk / Chat)
    if ((viewMode == VIEW_FUNK_HISTORY || viewMode == VIEW_CHAT_HISTORY) && canRunNavAction()) {
      if (escPressed) {
        viewMode = VIEW_HOME;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed(';')) {
        if (viewMode == VIEW_FUNK_HISTORY && !funkHistory.empty()) {
          selectedFunkIdx = (selectedFunkIdx - 1 + (int)funkHistory.size()) % (int)funkHistory.size();
        } else if (viewMode == VIEW_CHAT_HISTORY && !chatHistory.empty()) {
          selectedChatIdx = (selectedChatIdx - 1 + (int)chatHistory.size()) % (int)chatHistory.size();
        }
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('.')) {
        if (viewMode == VIEW_FUNK_HISTORY && !funkHistory.empty()) {
          selectedFunkIdx = (selectedFunkIdx + 1) % (int)funkHistory.size();
        } else if (viewMode == VIEW_CHAT_HISTORY && !chatHistory.empty()) {
          selectedChatIdx = (selectedChatIdx + 1) % (int)chatHistory.size();
        }
        flagUiDirty = true;
        return;
      }
      if (ks.enter && canSendEnter()) {
        if (viewMode == VIEW_FUNK_HISTORY && !funkHistory.empty()) {
          detailIndex = selectedFunkIdx;
          viewMode = VIEW_FUNK_DETAIL;
          flagUiDirty = true;
          return;
        }
        if (viewMode == VIEW_CHAT_HISTORY && !chatHistory.empty()) {
          detailIndex = selectedChatIdx;
          viewMode = VIEW_CHAT_DETAIL;
          flagUiDirty = true;
          return;
        }
      }
    }

    // Kurzbefehle auf dem Hauptbildschirm
    if (canRunNavAction()) {
      if (M5Cardputer.Keyboard.isKeyPressed('h') || M5Cardputer.Keyboard.isKeyPressed('H')) {
        brightnessIdx = (brightnessIdx + 1) % 4;
        applyBrightnessLevel();
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('1')) {
        textInput = "";
        viewMode = VIEW_CHAT_INPUT;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('2')) {
        sendTextPacket(PKT_RING, currentTarget, "RING");
        systemStatus = String("Klingel -> ") + ROOM_NAMES[currentTarget];
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('3')) {
        textInput = roomPlans[currentTarget == ROOM_ALL ? myRoom : currentTarget];
        planEditTarget = (currentTarget == ROOM_ALL) ? myRoom : currentTarget;
        viewMode = VIEW_PLAN_EDIT;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('4')) {
        viewMode = VIEW_ALARM_EDIT;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('5')) {
        currentTarget = (RoomId)(((uint8_t)currentTarget + 1) % ROOM_COUNT);
        systemStatus = String("Ziel: ") + ROOM_NAMES[currentTarget];
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('p') || M5Cardputer.Keyboard.isKeyPressed('P')) {
        viewMode = VIEW_PLAN_WINDOW;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('f') || M5Cardputer.Keyboard.isKeyPressed('F')) {
        viewMode = (viewMode == VIEW_FUNK_HISTORY) ? VIEW_HOME : VIEW_FUNK_HISTORY;
        flagUiDirty = true;
        return;
      }
      if (M5Cardputer.Keyboard.isKeyPressed('c') || M5Cardputer.Keyboard.isKeyPressed('C')) {
        viewMode = (viewMode == VIEW_CHAT_HISTORY) ? VIEW_HOME : VIEW_CHAT_HISTORY;
        flagUiDirty = true;
        return;
      }
    }
  }

  if (flagUiDirty && !isRecordingVoice) {
    drawMainScreen();
  }

  delay(5);
}