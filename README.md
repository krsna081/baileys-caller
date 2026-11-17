# baileys-caller

WhatsApp VoIP calling SDK for Node.js. Built on Baileys, audio & video via WhatsApp Web's WASM stack.

Maintained and upgraded by **Krisna Ady S. (krsna081)**.

## Features
- 📞 **1-on-1 Voice Calling**: Make and receive voice calls via WhatsApp VoIP.
- 📹 **1-on-1 Video Calling**: Stream video or static thumbnails with ffmpeg into active calls.
- 🖥️ **Screen Sharing**: Stream desktop/screen share feeds in real-time.
- 👥 **Group Calling**: Full support for group voice and video calls with multi-device participant routing.
- 🎙️ **Voice Chat (Obrolan Suara)**: Start or join lightweight group rooms without ringing all members.
- 🎵 **Audio Feeder with Queue & Playlist**: Enqueue songs, skip tracks, and auto-play seamlessly.

## Installation

```bash
npm install github:krsna081/baileys-caller
```

Peer dependency: `@whiskeysockets/baileys` (or compatible Baileys fork).

## Quick Start

```javascript
import { makeVoipClient } from 'baileys-caller';

// Using an existing Baileys socket:
const voipClient = await makeVoipClient(sock);

// 1-on-1 Voice Call
const call = await voipClient.call("6281234567890", {
  audioSource: "./song.mp3"
});

// Group Call / Voice Chat
const groupCall = await voipClient.callGroup("120363xxx@g.us", {
  audioSource: "./song.mp3",
  isLightWeight: true // Obrolan Suara
});

// Join ongoing Voice Chat
const joinedCall = await voipClient.joinGroupCall({
  callId: ongoing.callId,
  callCreatorJid: ongoing.callCreatorJid,
  initialPeerJid: ongoing.initialPeerJid,
  groupJid: "120363xxx@g.us",
  rawNode: ongoing.rawNode // optional cached offer node
}, {
  audioSource: "./song.mp3"
});
```

## License
MIT
