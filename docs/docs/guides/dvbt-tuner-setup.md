---
title: DVB-T Tuner Setup in TVHeadend
---

# DVB-T Tuner Setup in TVHeadend

This guide walks through adding a DVB tuner directly in the TVHeadend UI when running Headendarr AIO.

It covers:

- passing `/dev/dvb` into the container
- enabling the adapter in TVHeadend
- creating a DVB-T/T2 network
- scanning muxes and mapping services to channels

## Prerequisites

- Headendarr AIO is running.
- Your host OS can see the tuner (for example under `/dev/dvb/adapter0`).
- Antenna/cable is connected to the tuner.

## 1. Pass `/dev/dvb` Into the Container

TVHeadend cannot see DVB hardware unless the device nodes are available inside the container.

### Docker Compose

Add this to your Headendarr service:

```yaml
services:
  headendarr:
    devices:
      - /dev/dvb:/dev/dvb
```

If you also use hardware acceleration, add:

```yaml
devices:
  - /dev/dvb:/dev/dvb
  - /dev/dri:/dev/dri
```

### Unraid (Extra Parameters)

In **Advanced View**, set **Extra Parameters**:

```text
--device=/dev/dvb:/dev/dvb
```

Optional with GPU:

```text
--device=/dev/dvb:/dev/dvb --device=/dev/dri:/dev/dri
```

### Verify from Container Shell

```bash
ls -la /dev/dvb
ls -la /dev/dvb/adapter0
```

You should see character devices like `frontend0`, `demux0`, and `dvr0`.

## 2. Open TVHeadend UI

All steps in this guide are done in the TVHeadend backend UI.

1. In Headendarr, click **Show TVHeadend Backend** in the header.
2. This opens the TVHeadend UI in a popup dialog.
3. Continue in that popup (or open it in a separate tab/window if preferred).
4. Go to `Configuration -> DVB Inputs -> TV adapters`.
5. Confirm your adapter appears.

![TV adapters page with discovered adapter](/img/screenshots/tvh-dvb-setup-adapter-found.png)

If it does not appear:

- re-check `/dev/dvb` mapping
- restart the container
- verify host has loaded tuner drivers

## 3. Enable Adapter

1. Click your adapter in `TV adapters`.
2. Tick **Enabled**.
3. Save.

## 4. Create DVB-T/T2 Network

1. Go to `Configuration -> DVB Inputs -> Networks`.
2. Click **Add** and choose **DVB-T Network**.

![Add DVB-T network dialog](/img/screenshots/tvh-dvb-setup-click-add-network.png)

3. Configure:
   - **Network name**: e.g. `dvb-t-local`
   - **Pre-defined muxes**: choose your country/region transmitter list
   - Leave scan options at defaults to start

![Configure network for local region defaults](/img/screenshots/tvh-dvb-setup-configure-network-for-region.png)

4. Click **Save**.

## 5. Assign the Network to the Adapter

After creating the network, you must apply it to your tuner adapter.

1. Go back to `Configuration -> DVB Inputs -> TV adapters`.
2. Select your tuner adapter.
3. In **Networks**, select `dvb-t-local` (or whatever network name you created).

![Adapter enabled and assigned to DVB network](/img/screenshots/tvh-dvb-setup-enable-adapter-on-network.png)

4. Click **Save**.

## 6. Scan Muxes

1. Go to `Configuration -> DVB Inputs -> Muxes`.
2. Wait for scan to progress.
3. Confirm muxes move toward **OK** state.

![Muxes created and pending initial scan](/img/screenshots/tvh-dvb-setup-muxes-created-pending.png)

If all muxes fail:

- confirm antenna signal
- confirm correct transmitter/region list
- test with a narrower or correct local pre-defined mux set

## 7. Review Discovered Services

1. Go to `Configuration -> DVB Inputs -> Services`.
2. Filter by your DVB network/adapter.
3. Confirm services are discovered.

## 8. Map Services to Channels

1. In `Services`, select the services you want.
2. Click **Map selected** (or map all).
3. Start with defaults, then adjust as needed:
   - create provider tags
   - merge options
   - channel number policy
4. Save.

Channels will now appear under `Configuration -> Channel / EPG -> Channels`.

## 9. Optional: EPG and OTA Metadata

For DVB broadcast EPG:

1. Go to `Configuration -> Channel / EPG -> EPG Grabber Modules`.
2. Enable relevant OTA modules (for your region/standard).
3. Confirm guide data starts populating.

You can also continue using XMLTV via Headendarr if preferred.

## 10. Validate Playback

1. Test playback in TVHeadend web player or your client.
2. Confirm audio/video starts quickly and is stable.
3. Check TVHeadend logs for tuner lock/signal errors if needed.

## Notes for Headendarr Users

- Headendarr-managed IPTV networks are normally prefixed with `tic-`.
- For manually managed DVB networks, use your own naming (for example `dvb-t-local`) and manage them in TVHeadend directly.
- Avoid renaming manual DVB networks to `tic-*` to prevent confusion with Headendarr-managed objects.
