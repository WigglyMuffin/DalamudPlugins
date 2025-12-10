# DalamudPlugins

[![Discord](https://img.shields.io/badge/Discord-Join%20Server-5865F2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/pngyvpYVt2)

---

### Questionable
- **Author**: WigglyMuffin, CryoTechnic
- **Description**: Originally created by Liza Carvelli, now maintained and improved by WigglyMuffin and CryoTechnic. A tiny little quest helper plugin that automates quests where possible. Uses navmesh to automatically walk to all quest waypoints, and tries to automatically complete all steps along the way.
- **Features**: 
  - Automatic navigation using navmesh
  - Automated quest step completion
  - MSQ and quest automation support
- **Note**: Not all quests are supported, check Discord for support.
- **Required Dependencies**: [vnavmesh](https://github.com/awgil/ffxiv_navmesh/), [Lifestream](https://github.com/NightmareXIV/Lifestream), [TextAdvance](https://github.com/NightmareXIV/TextAdvance)
- **Recommended**: For the best experience, use with our partnered **Questionable Companion** plugin (see below)

### Questionable Companion
- **Author**: MacaronDream
- **Description**: Automated character rotation and quest sequence execution for Questionable. Automates multi-character MSQ progression by intelligently switching between characters based on quest completion and level requirements.
- **Features**:
  - Automatic character switching and quest tracking
  - MSQ progression monitoring across all characters
  - Allied Society quest rotation and prioritization
  - Event quest automation with prerequisite checking
  - Chauffeur Mode for following main character through quests
  - Data Center travel automation
  - Stop point configuration for controlled progression
- **Required Dependencies**: [Questionable](https://github.com/WigglyMuffin/Questionable/), [AutoRetainer](https://github.com/PunishXIV/AutoRetainer)

### Influx
- **Author**: WigglyMuffin
- **Description**: Originally created by Liza Carvelli, now maintained and improved by WigglyMuffin. Uploads your game statistics (such as quest progress, gil, retainer stats) to an InfluxDB or QuestDB instance, making it possible to automatically track your progress through external tools such as Grafana.
- **Features**:
  - Sync quest progress, gil, and retainer stats
  - InfluxDB integration
  - QuestDB integration
  - Grafana visualization support

## Installation

Add this repository URL to your Dalamud custom plugin repositories:
```
https://raw.githubusercontent.com/WigglyMuffin/DalamudPlugins/main/pluginmaster.json
```
