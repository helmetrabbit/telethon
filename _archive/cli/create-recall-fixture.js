
const fs = require('fs');
const path = require('path');

const src = path.join(__dirname, '../../data/exports/telethon_bd_web3.json');
const dest = path.join(__dirname, '../../data/exports/recall_test.json');

const data = JSON.parse(fs.readFileSync(src, 'utf-8'));

// Take top 10 participants
data.participants = data.participants.slice(0, 10);
// Clear messages for speed/simplicity or keep a few
data.messages = data.messages.slice(0, 50);

// Inject Bios
const bioMap = [
    { id: 424278354, bio: "Founder of FlashTrade. Building high-speed perps." }, // Founder + Builder
    { id: 1314436676, bio: "Head of Growth at Avalanche. DM for partnerships." }, // BD
    { id: 1167327, bio: "Solidity Engineer @ Aave. Auditing smart contracts." }, // Builder + Audit
    { id: 1656094770, bio: "Just a random guy." }, // Control
    { id: 5936367339, bio: "Investigating new primitive. Researcher at Paradigm." }, // Investor
];

data.participants.forEach(p => {
    const override = bioMap.find(b => b.id === p.user_id);
    if (override) {
        p.about = override.bio;
        console.log(`Injecting bio for ${p.user_id}: ${p.about}`);
    }
});

// Rename group to avoid collision
data.name = "Recall Test Group " + Date.now();
data.id = 999999990 + Math.floor(Math.random() * 10); 

fs.writeFileSync(dest, JSON.stringify(data, null, 2));
console.log('Created recall_test.json');
