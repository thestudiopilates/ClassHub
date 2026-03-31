const fallbackData = {
  summary: [
    { label: "Class live now", value: 2 },
    { label: "People checked in", value: 4 },
    { label: "New clients today", value: 2 },
    { label: "Milestones today", value: 4 },
    { label: "Birthdays today", value: 1 },
  ],
  freshness: [
    { domain: "active clients", status: "fresh", note: "updated 8m ago" },
    { domain: "birthdays", status: "fresh", note: "updated at 5:00 AM" },
    { domain: "customer fields", status: "fresh", note: "nightly refresh" },
    { domain: "memberships + notes", status: "fresh", note: "updated 14m ago" },
    { domain: "bookings", status: "stale", note: "Updates at 10:00 AM" },
  ],
  celebrations: [
    "Monica Abdelmalak · 50th class today",
    "Priya Shah · First class back after baby",
    "Sarah Abshire · Birthday on Thursday",
    "Rayah Abudayyeh · 100th class next week",
  ],
  people: {
    "michael-baddour": {
      id: "michael-baddour",
      name: "Michael Baddour",
      membership: "10 Class Pack",
      funFact: "I'm new to Pilates!",
      churnRisk: {
        level: "high",
        reason: "Booked 1 class this month versus 4 in his first month after joining.",
        rule: "High risk when current-month bookings are down 50%+ versus first-month baseline.",
      },
      profile: {
        firstName: "Michael",
        fullName: "Michael Baddour",
        subtext: "New client energy. He needs confidence, consistency, and a reason to come back this week.",
        details: [
          { label: "How heard about us", value: "Walking By" },
          { label: "Fun fact", value: "I'm new to Pilates!" },
          { label: "Preferred format", value: "Power Flow" },
          { label: "Preferred time", value: "Morning" },
          { label: "Membership", value: "10 Class Pack" },
          { label: "Churn risk", value: "High" },
        ],
        chips: ["Welcome intentionally", "Offer next booking help", "Affirm progress after class"],
        notes: ["Low visit count with recent gap. Great candidate for a specific rebooking prompt."],
      },
    },
    "monica-abdelmalak": {
      id: "monica-abdelmalak",
      name: "Monica Abdelmalak",
      membership: "50 Class Celebration Bundle!",
      funFact: "I have an adorable Corgi named Maple",
      churnRisk: {
        level: "low",
        reason: "Bookings are steady versus last month and her normal pattern.",
        rule: "Low risk when current-month bookings are within 10% of previous month.",
      },
      profile: {
        firstName: "Monica",
        fullName: "Monica Abdelmalak",
        subtext: "This should feel like a celebration moment, not just another check-in.",
        details: [
          { label: "How heard about us", value: "Google Search" },
          { label: "Fun fact", value: "I have an adorable Corgi named Maple" },
          { label: "Favorite instructor", value: "Autumn" },
          { label: "Favorite format", value: "Power Flow" },
          { label: "Membership", value: "50 Class Celebration Bundle!" },
          { label: "Churn risk", value: "Low" },
        ],
        chips: ["Celebrate milestone", "Ask about Maple", "Invite referral moment"],
        notes: ["Highly engaged regular. Celebration and recognition will land extremely well."],
      },
    },
    "priya-shah": {
      id: "priya-shah",
      name: "Priya Shah",
      membership: "8x Month",
      funFact: "Collects vintage cookbooks",
      churnRisk: {
        level: "medium",
        reason: "Bookings are down versus her last active month while she is returning after baby.",
        rule: "Medium risk when current-month bookings are down 20-49% versus previous active month.",
      },
      profile: {
        firstName: "Priya",
        fullName: "Priya Shah",
        subtext: "Important return moment. She is back postpartum and likely checking how her body feels.",
        details: [
          { label: "How heard about us", value: "Word of Mouth" },
          { label: "Fun fact", value: "Collects vintage cookbooks" },
          { label: "Recent milestone", value: "First class back after baby" },
          { label: "Preferred instructor", value: "Autumn" },
          { label: "Membership", value: "8x Month" },
          { label: "Churn risk", value: "Medium" },
        ],
        chips: ["Gentle confidence", "Celebrate return", "Offer modifications first"],
        notes: ["Front desk and instructor should both make this feel supported, not scrutinized."],
      },
    },
    "sarah-abshire": {
      id: "sarah-abshire",
      name: "Sarah Abshire",
      membership: "8x Month",
      funFact: "I've hiked 2 volcanoes!",
      churnRisk: {
        level: "low",
        reason: "Booking rhythm is consistent with last month.",
        rule: "Low risk when current-month bookings are flat or up versus previous month.",
      },
      profile: {
        firstName: "Sarah",
        fullName: "Sarah Abshire",
        subtext: "A warm birthday touch and a little personality reference will go a long way.",
        details: [
          { label: "How heard about us", value: "Google Search" },
          { label: "Fun fact", value: "I've hiked 2 volcanoes!" },
          { label: "Birthday", value: "This week" },
          { label: "Preferred instructor", value: "Minji" },
          { label: "Membership", value: "8x Month" },
          { label: "Churn risk", value: "Low" },
        ],
        chips: ["Birthday touchpoint", "Ask about travel plans", "Steady regular"],
        notes: ["No intervention needed. Make the experience feel personal and bright."],
      },
    },
    "rayah-abudayyeh": {
      id: "rayah-abudayyeh",
      name: "Rayah Abudayyeh",
      membership: "12x Month",
      funFact: "Went to the same high school as Yung Gravy",
      churnRisk: {
        level: "low",
        reason: "Bookings are stable and milestone momentum is building.",
        rule: "Low risk when current-month bookings are at or above previous month.",
      },
      profile: {
        firstName: "Rayah",
        fullName: "Rayah Abudayyeh",
        subtext: "Loyal, playful personality. Great person to energize the room.",
        details: [
          { label: "How heard about us", value: "Walking By" },
          { label: "Fun fact", value: "Went to the same high school as Yung Gravy" },
          { label: "Upcoming milestone", value: "100th class next week" },
          { label: "Preferred time", value: "Morning" },
          { label: "Membership", value: "12x Month" },
          { label: "Churn risk", value: "Low" },
        ],
        chips: ["Prime for celebration", "High energy anchor", "Flag for 100th soon"],
        notes: ["She is a culture carrier. Use that energy in class."],
      },
    },
    "anjali-abrahams": {
      id: "anjali-abrahams",
      name: "Anjali Abrahams",
      membership: "8x Month",
      funFact: "I'm a private chef",
      churnRisk: {
        level: "low",
        reason: "No meaningful reduction in bookings versus last month.",
        rule: "Low risk when month-over-month bookings remain in healthy range.",
      },
      profile: {
        firstName: "Anjali",
        fullName: "Anjali Abrahams",
        subtext: "This relationship benefits from context and warmth more than selling.",
        details: [
          { label: "How heard about us", value: "Class Pass" },
          { label: "Fun fact", value: "I'm a private chef" },
          { label: "Preferred instructor", value: "Rosalynn" },
          { label: "Preferred format", value: "Foundations" },
          { label: "Membership", value: "8x Month" },
          { label: "Churn risk", value: "Low" },
        ],
        chips: ["Warm welcome", "Service-sensitive", "Keep it easy today"],
        notes: ["Prior service context suggests empathy matters more than process."],
      },
    },
  },
  frontdesk: [
    {
      id: "michael-baddour",
      arrival: "8:00 AM · Power Flow",
      bookingId: "bk-michael-001",
      notes: ["New client", "Needs rebooking support"],
      metrics: [
        { label: "Visits", value: "3" },
        { label: "Last seen", value: "21d" },
        { label: "Membership", value: "Pack" },
      ],
      badges: [
        { label: "New client", tone: "info" },
        { label: "High risk", tone: "risk" },
      ],
    },
    {
      id: "monica-abdelmalak",
      arrival: "8:00 AM · Power Flow",
      bookingId: "bk-monica-001",
      notes: ["50th class", "Maple the corgi"],
      metrics: [
        { label: "Visits", value: "49" },
        { label: "Last seen", value: "2d" },
        { label: "Membership", value: "Bundle" },
      ],
      badges: [
        { label: "Milestone", tone: "positive" },
        { label: "Low risk", tone: "positive" },
      ],
    },
    {
      id: "priya-shah",
      arrival: "8:00 AM · Power Flow",
      bookingId: "bk-priya-001",
      notes: ["First class back after baby", "Offer modifications first"],
      metrics: [
        { label: "Visits", value: "26" },
        { label: "Last seen", value: "4mo" },
        { label: "Membership", value: "8x" },
      ],
      badges: [
        { label: "Return after baby", tone: "birthday" },
        { label: "Medium risk", tone: "risk" },
      ],
    },
  ],
  sessions: [
    {
      id: "sess-001",
      title: "Power Flow",
      time: "Now · 8:00 AM",
      instructor: "Autumn",
      location: "Emory Village",
      summary: [
        { label: "In class", value: "6" },
        { label: "Milestones", value: "2" },
        { label: "Birthdays", value: "1" },
        { label: "Special returns", value: "1" },
      ],
      highlights: ["50th class today", "Birthday this week", "First class back after baby"],
      roster: [
        {
          personId: "michael-baddour",
          bookingId: "bk-michael-001",
          checkedIn: false,
          badges: [
            { label: "New client", tone: "info" },
            { label: "High churn risk", tone: "risk" },
          ],
          visibleHighlights: [
            { label: "Assumption", value: "May need extra reassurance and clear reformer setup cues." },
            { label: "Quick note", value: "3 total visits. Great candidate for an encouraging post-class check-in." },
          ],
          stats: [
            { label: "Class count", value: "3" },
            { label: "Favorite time", value: "Morning" },
            { label: "Membership", value: "Pack" },
          ],
          expand: {
            assumption: "Likely still deciding whether this is a routine. Positive feedback will matter.",
            service: "Front desk should offer help booking the next class before he leaves.",
            notes: ["Fun fact: I'm new to Pilates!", "Lead source: Walking By"],
          },
        },
        {
          personId: "monica-abdelmalak",
          bookingId: "bk-monica-001",
          checkedIn: true,
          badges: [
            { label: "50th class", tone: "positive" },
            { label: "Celebrate today", tone: "birthday" },
          ],
          visibleHighlights: [
            { label: "Milestone", value: "49 visits logged. Today should feel like the 50th class moment." },
            { label: "Fun fact", value: "Has an adorable corgi named Maple." },
          ],
          stats: [
            { label: "Class count", value: "49" },
            { label: "Favorite instructor", value: "Autumn" },
            { label: "Risk", value: "Low" },
          ],
          expand: {
            assumption: "A visible, happy acknowledgment will deepen loyalty more than a generic compliment.",
            service: "Have front desk congratulate her on the way out and suggest a photo moment if appropriate.",
            notes: ["How heard about us: Google Search", "Strong referral candidate"],
          },
        },
        {
          personId: "priya-shah",
          bookingId: "bk-priya-001",
          checkedIn: false,
          badges: [
            { label: "First class back after baby", tone: "birthday" },
            { label: "Offer modifications", tone: "info" },
          ],
          visibleHighlights: [
            { label: "Return marker", value: "Postpartum return. First class back after baby." },
            { label: "Assumption", value: "Will likely appreciate options, pacing, and reassurance." },
          ],
          stats: [
            { label: "Class count", value: "26" },
            { label: "Gap", value: "4 months" },
            { label: "Risk", value: "Medium" },
          ],
          expand: {
            assumption: "Energy may be cautious today. Make her feel welcome without putting her on the spot.",
            service: "Front desk should celebrate her return gently and ask how class felt afterward.",
            notes: ["Fun fact: Collects vintage cookbooks", "Membership: 8x Month"],
          },
        },
        {
          personId: "rayah-abudayyeh",
          bookingId: "bk-rayah-001",
          checkedIn: false,
          badges: [
            { label: "100th class soon", tone: "positive" },
            { label: "Room energy", tone: "info" },
          ],
          visibleHighlights: [
            { label: "Milestone", value: "100th class next week. Start building momentum now." },
            { label: "Fun fact", value: "Went to the same high school as Yung Gravy." },
          ],
          stats: [
            { label: "Class count", value: "96" },
            { label: "Favorite time", value: "Morning" },
            { label: "Risk", value: "Low" },
          ],
          expand: {
            assumption: "This is a loyal client with personality. Let her help set a bright tone.",
            service: "Front desk can quietly prep for a 100th celebration next week.",
            notes: ["Lead source: Walking By", "High-energy regular"],
          },
        },
      ],
    },
    {
      id: "sess-002",
      title: "Sculpt Reformer",
      time: "Up next · 9:15 AM",
      instructor: "Minji",
      location: "Emory Village",
      summary: [
        { label: "Booked", value: "5" },
        { label: "Birthdays", value: "1" },
        { label: "Milestones", value: "1" },
        { label: "New clients", value: "0" },
      ],
      highlights: ["Birthday this week", "Chef fun fact", "Warm regulars"],
      roster: [
        {
          personId: "sarah-abshire",
          bookingId: "bk-sarah-001",
          checkedIn: false,
          badges: [
            { label: "Birthday week", tone: "birthday" },
            { label: "Steady regular", tone: "positive" },
          ],
          visibleHighlights: [
            { label: "Birthday", value: "Birthday this week. A little acknowledgment will land well." },
            { label: "Fun fact", value: "Has hiked two volcanoes." },
          ],
          stats: [
            { label: "Class count", value: "22" },
            { label: "Preferred time", value: "Mid-morning" },
            { label: "Risk", value: "Low" },
          ],
          expand: {
            assumption: "No intervention needed. She is a healthy regular who will appreciate a personal touch.",
            service: "Front desk can offer a birthday retail perk suggestion.",
            notes: ["Lead source: Google Search", "Favorite instructor: Minji"],
          },
        },
        {
          personId: "anjali-abrahams",
          bookingId: "bk-anjali-001",
          checkedIn: false,
          badges: [
            { label: "Service-sensitive", tone: "info" },
            { label: "Private chef", tone: "birthday" },
          ],
          visibleHighlights: [
            { label: "Assumption", value: "Warmth and low-friction service matter more than pushing offers." },
            { label: "Fun fact", value: "Private chef." },
          ],
          stats: [
            { label: "Class count", value: "14" },
            { label: "Preferred format", value: "Foundations" },
            { label: "Risk", value: "Low" },
          ],
          expand: {
            assumption: "Teach normally, but keep the tone easy and welcoming.",
            service: "Reference any prior courtesy gracefully if she brings it up.",
            notes: ["Lead source: Class Pass", "Membership: 8x Month"],
          },
        },
      ],
    },
  ],
};

function buildInitialCheckInState() {
  const statusByBookingId = {};
  fallbackData.sessions.forEach((session) => {
    session.roster.forEach((entry) => {
      if (entry.bookingId) {
        statusByBookingId[entry.bookingId] = {
          checkedIn: Boolean(entry.checkedIn),
          pending: false,
          message: entry.checkedIn ? "Already checked in." : "Ready to check in.",
          tone: entry.checkedIn ? "success" : "neutral",
        };
      }
    });
  });
  return statusByBookingId;
}

function buildCheckInStateFromData(sourceData) {
  const statusByBookingId = {};
  (sourceData.sessions || []).forEach((session) => {
    (session.roster || []).forEach((entry) => {
      if (entry.bookingId) {
        statusByBookingId[entry.bookingId] = {
          checkedIn: Boolean(entry.checkedIn),
          pending: false,
          message: entry.checkedIn ? "Already checked in." : "Ready to check in.",
          tone: entry.checkedIn ? "success" : "neutral",
        };
      }
    });
  });
  return statusByBookingId;
}

let currentData = { meta: {}, summary: [], freshness: [], celebrations: [], people: {}, frontdesk: [], sessions: [] };

const state = {
  view: "instructor",
  location: "emory",
  selectedSessionId: null,
  expandedRosterIds: [],
  query: "",
  checkInByBookingId: {},
};

const summaryStats = document.getElementById("summary-stats");
const freshnessList = document.getElementById("freshness-list");
const celebrations = document.getElementById("celebrations");
const frontdeskGrid = document.getElementById("frontdesk-grid");
const sessionList = document.getElementById("session-list");
const sessionRoster = document.getElementById("session-roster");
const summaryHeading = document.getElementById("summary-heading");
const searchInput = document.getElementById("search-input");
const locationButtons = Array.from(document.querySelectorAll(".location-pill"));
const liveDayNote = document.getElementById("live-day-note");

function currentDayLabel() {
  const provided = currentData.meta?.dayLabel;
  if (provided) return provided;
  const source = currentData.meta?.day || new Date().toISOString().slice(0, 10);
  const [year, month, day] = source.split("-").map(Number);
  const dt = new Date(year, (month || 1) - 1, day || 1);
  return dt.toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
  });
}

function getPerson(personId) {
  return currentData.people[personId] || { id: personId, name: personId, membership: "", funFact: "", churnRisk: {}, profile: { firstName: "", fullName: personId, subtext: "", details: [], chips: [], notes: [] } };
}

function locationLabel() {
  return state.location === "west-midtown" ? "West Midtown" : "Emory";
}

function getFilteredFrontdeskItems() {
  return currentData.frontdesk.filter((item) => {
    const person = getPerson(item.id);
    return matchesSelectedLocation(item.location) && matchesQuery([person.name, item.arrival, person.membership, item.notes.join(" "), item.location || ""]);
  });
}

function getFilteredSessions() {
  return currentData.sessions.filter(
    (session) =>
      matchesSelectedLocation(session.location) &&
      matchesQuery([session.title, session.time, session.instructor, session.location, session.roster.map((item) => getPerson(item.personId).name).join(" ")]),
  );
}

function buildLocationSummary() {
  const sessions = getFilteredSessions();
  const checkedInPeople = new Set();
  const newClientsToday = new Set();
  const milestonePeople = new Set();
  const birthdayPeople = new Set();

  for (const session of sessions) {
    for (const rosterItem of session.roster || []) {
      if (!rosterItem?.personId) continue;
      if (rosterItem.checkedIn) {
        checkedInPeople.add(rosterItem.personId);
      }
      if (Number(rosterItem.classNumberToday) === 1) {
        newClientsToday.add(rosterItem.personId);
      }
      if (rosterItem.bookingMilestone) {
        milestonePeople.add(rosterItem.personId);
      }
      if (rosterItem.birthdayToday) {
        birthdayPeople.add(rosterItem.personId);
      }
    }
  }

  return [
    { label: "Class live now", value: sessions.length },
    { label: "People checked in", value: checkedInPeople.size },
    { label: "New clients today", value: newClientsToday.size },
    { label: "Milestones today", value: milestonePeople.size },
    { label: "Birthdays today", value: birthdayPeople.size },
  ];
}

function renderSummary() {
  const locationSummary = buildLocationSummary();
  const hasLiveData = Boolean(currentData.meta?.liveBookings);
  const allZero = locationSummary.every((item) => item.value === 0);
  const stats = hasLiveData && allZero ? currentData.summary : locationSummary;
  summaryHeading.textContent = `${locationLabel()} snapshot`;
  summaryStats.innerHTML = stats
    .map(
      (item, index) => `
        <article class="stat-card ${index === 3 && Number(item.value) > 0 ? "is-milestones" : index === 2 ? "is-milestones" : ""}">
          <span class="metric-label">${item.label}</span>
          <strong>${item.value}</strong>
        </article>
      `,
    )
    .join("");

  freshnessList.innerHTML = currentData.freshness
    .map(
      (item) => `
        <div class="freshness-row ${item.status === "stale" ? "is-stale" : ""}">
          <div>
            <strong class="freshness-domain">${item.domain}</strong>
            <div class="freshness-note">${item.note}</div>
          </div>
          <span class="freshness-pill ${item.status === "fresh" ? "is-fresh" : "is-stale"}">${item.status}</span>
        </div>
      `,
    )
    .join("");

  celebrations.innerHTML = currentData.celebrations
    .map((item) => `<li><span class="celebration-icon">${celebrationIcon(item)}</span><span>${item}</span></li>`)
    .join("");
}

function toneClass(tone) {
  if (tone === "risk") return "is-risk";
  if (tone === "positive") return "is-positive";
  if (tone === "birthday") return "is-birthday";
  if (tone === "milestone") return "is-milestone";
  if (tone === "identity") return "is-identity";
  return "is-info";
}

function detailTone(label) {
  const normalized = label.toLowerCase();
  if (normalized.includes("fun fact")) return "is-fun-fact";
  if (normalized.includes("milestone") || normalized.includes("birthday")) return "is-milestone";
  if (normalized.includes("churn")) return "is-churn";
  return "";
}

function highlightTone(label) {
  const normalized = label.toLowerCase();
  if (normalized.includes("fun fact") || normalized.includes("identity")) return "is-identity";
  if (normalized.includes("milestone") || normalized.includes("birthday") || normalized.includes("return")) {
    return "is-milestone";
  }
  if (normalized.includes("churn")) return "is-risk";
  return "";
}

function matchesQuery(parts) {
  if (!state.query) return true;
  return parts.join(" ").toLowerCase().includes(state.query);
}

function normalizeLocationName(value) {
  return (value || "").toLowerCase();
}

function matchesSelectedLocation(value) {
  const normalized = normalizeLocationName(value);
  if (!normalized) return true;
  if (state.location === "emory") return normalized.includes("emory");
  if (state.location === "west-midtown") return normalized.includes("west midtown") || normalized.includes("w. midtown");
  return true;
}

function riskLevelClass(level, isNew = false) {
  if (isNew || !level) return "is-risk-new";
  if (level === "high") return "is-risk-high";
  if (level === "medium") return "is-risk-medium";
  return "is-risk-low";
}

function celebrationIcon(text) {
  const normalized = text.toLowerCase();
  if (normalized.includes("birthday")) return "🎂";
  if (normalized.includes("back after baby") || normalized.includes("return")) return "💛";
  return "🎉";
}

function isNewClient(person, item) {
  const joinedText = `${person.membership} ${item.notes?.join(" ") || ""} ${item.badges?.map((badge) => badge.label).join(" ") || ""}`.toLowerCase();
  return joinedText.includes("new client");
}

function buildAlertModel(person, item) {
  const level = person.churnRisk?.level;
  const highlights = item.visibleHighlights || [];
  const notes = item.notes || [];
  const milestone = item.badges?.find((badge) => badge.label.toLowerCase().includes("class") || badge.label.toLowerCase().includes("milestone"));
  const birthday = item.badges?.find((badge) => badge.label.toLowerCase().includes("birthday"));
  const returnBadge = item.badges?.find((badge) => badge.label.toLowerCase().includes("return") || badge.label.toLowerCase().includes("back after baby"));

  let primary = null;
  if (level === "high") {
    primary = {
      tone: "risk",
      label: "High churn risk",
      text: person.churnRisk.reason,
    };
  } else if (milestone || birthday) {
    primary = {
      tone: "milestone",
      label: milestone ? milestone.label : birthday.label,
      text: highlights.find((highlight) => /milestone|birthday/i.test(highlight.label))?.value || "Celebrate this client today.",
    };
  } else if (returnBadge) {
    primary = {
      tone: "return",
      label: returnBadge.label,
      text: highlights.find((highlight) => /return/i.test(highlight.label))?.value || "Supportive return moment.",
    };
  }

  const pills = [];
  if (person.funFact) {
    pills.push({ tone: "fun-fact", html: `<strong>Fun fact:</strong> <span>${person.funFact}</span>` });
  }
  if (level === "medium") {
    pills.push({ tone: "note", text: "Medium churn risk" });
  } else if (level === "low") {
    pills.push({ tone: "low-risk", text: "Low churn risk" });
  }
  notes.slice(0, 2).forEach((note) => pills.push({ tone: "note", text: note }));

  return { primary, pills };
}

function renderFrontdesk() {
  const cards = getFilteredFrontdeskItems();

  frontdeskGrid.style.gridTemplateColumns =
    cards.length === 2 ? "repeat(2, minmax(0, 1fr))" : cards.length >= 3 ? "repeat(3, minmax(0, 1fr))" : "1fr";

  if (!cards.length) {
    frontdeskGrid.innerHTML = `<div class="empty-state"><p>No live front desk arrivals yet. Once bookings are loaded, this queue will populate automatically.</p></div>`;
    return;
  }

  frontdeskGrid.innerHTML = cards
    .map((item) => {
      const person = getPerson(item.id);
      const checkIn = item.bookingId ? state.checkInByBookingId[item.bookingId] : null;
      const isNew = isNewClient(person, item);
      const alertModel = buildAlertModel(person, item);
      const frontdeskBadges = item.badges.filter((badge) => !/risk/i.test(badge.label));
      const badgeMarkup = frontdeskBadges.length
        ? `
            <div class="badge-row">
              ${frontdeskBadges.map((badge) => `<span class="badge ${toneClass(badge.tone)}">${badge.label}</span>`).join("")}
            </div>
          `
        : "";
      const pillMarkup = alertModel.pills.length
        ? `
            <div class="pill-row">
              ${alertModel.pills
                .map((pill) =>
                  pill.html
                    ? `<span class="alert-pill ${pill.tone}">${pill.html}</span>`
                    : `<span class="alert-pill ${pill.tone}">${pill.text}</span>`,
                )
                .join("")}
            </div>
          `
        : "";
      return `
        <article class="person-card ${riskLevelClass(person.churnRisk?.level, isNew)}">
          <header>
            <div>
              <div class="person-name">${person.name}</div>
              <div class="meta-line">
                <span>${item.arrival}</span>
                <span>•</span>
                <span>${person.membership}</span>
              </div>
            </div>
            ${badgeMarkup}
          </header>
          ${
            alertModel.primary
              ? `<div class="alert-bar is-${alertModel.primary.tone}"><strong>${alertModel.primary.label}:</strong> ${alertModel.primary.text}</div>`
              : ""
          }
          ${pillMarkup}
          <div class="metrics">
            ${item.metrics
              .map(
                (metric) => `
                  <div class="metric">
                    <strong>${metric.value}</strong>
                    <span class="metric-label">${metric.label}</span>
                  </div>
                `,
              )
              .join("")}
          </div>
          ${
            item.bookingId
              ? `
                <div class="checkin-row">
                  <button
                    class="checkin-toggle ${checkIn?.checkedIn ? "is-checked" : ""}"
                    type="button"
                    data-checkin-id="${item.bookingId}"
                    data-checkin-state="${checkIn?.checkedIn ? "checked-in" : "not-checked-in"}"
                    ${checkIn?.pending ? "disabled" : ""}
                  >
                    ${checkIn?.pending ? "Saving..." : checkIn?.checkedIn ? "Undo check-in" : "Check in"}
                  </button>
                  <span class="checkin-status ${checkIn?.tone ? `is-${checkIn.tone}` : ""}">
                    ${checkIn?.message || "Ready to check in."}
                  </span>
                </div>
              `
              : ""
          }
          <footer>
            <div class="service-prompt">${person.profile.chips?.[0] || "Warm, personal service recommended"}</div>
            <div class="frontdesk-context">${person.profile.subtext}</div>
          </footer>
        </article>
      `;
    })
    .join("");
}

function renderSessions() {
  const sessions = getFilteredSessions();
  liveDayNote.textContent = `Highlights stay visible on the roster for ${currentDayLabel()}, and the rest of the client summary expands downward right inside each card.`;

  if (!sessions.some((session) => session.id === state.selectedSessionId)) {
    state.selectedSessionId = sessions[0]?.id || "";
  }

  sessionList.innerHTML = `
    <div class="session-stack">
      ${sessions
        .map(
          (session) => `
            <article class="session-card ${session.id === state.selectedSessionId ? "is-active" : ""}" data-session-id="${session.id}">
              <header>
                <div>
                  <div class="session-title">${session.title}</div>
                  <div class="meta-line">
                    <span>${session.time}</span>
                    <span>•</span>
                    <span>${session.instructor}</span>
                  </div>
                </div>
              </header>
              <div class="chip-row">
                ${session.highlights.map((highlight) => `<span class="chip">${highlight}</span>`).join("")}
              </div>
            </article>
          `,
        )
        .join("")}
    </div>
  `;

  const selected = sessions.find((session) => session.id === state.selectedSessionId) || sessions[0];
  if (!selected) {
    const liveBookings = currentData.meta?.liveBookings;
    sessionRoster.innerHTML = `<div class="empty-state"><p>${
      liveBookings
        ? "No classes match this search yet."
        : "Live roster view will appear here after the safe bookings feed is connected."
    }</p></div>`;
    return;
  }
  state.selectedSessionId = selected.id;

  sessionRoster.innerHTML = `
    <div class="class-banner">
      <div class="class-banner-top">
        <div>
          <p class="eyebrow">${selected.location}</p>
          <h4>${selected.title}</h4>
          <p class="section-note">${currentDayLabel()} · ${selected.time} · ${selected.instructor}</p>
        </div>
        <div class="chip-row">
          ${selected.highlights.map((item) => `<span class="chip">${item}</span>`).join("")}
        </div>
      </div>
      <div class="class-banner-grid">
        ${selected.summary
          .map(
            (item) => `
              <div class="compact-stat">
                <strong>${item.value}</strong>
                <span class="metric-label">${item.label}</span>
              </div>
            `,
          )
          .join("")}
      </div>
    </div>
    <div class="roster-stack">
      ${selected.roster
        .map((entry) => {
          const person = getPerson(entry.personId);
          const expanded = state.expandedRosterIds.includes(entry.personId);
          const checkIn = entry.bookingId ? state.checkInByBookingId[entry.bookingId] : null;
          const isNew = isNewClient(person, entry);
          const alertModel = buildAlertModel(person, entry);
          const nonFunFactPills = alertModel.pills.filter((pill) => pill.tone !== "fun-fact");
          const badgeMarkup = entry.badges.length
            ? `
                <div class="badge-row">
                  ${entry.badges.map((badge) => `<span class="badge ${toneClass(badge.tone)}">${badge.label}</span>`).join("")}
                </div>
              `
            : "";
          const pillMarkup = nonFunFactPills.length
            ? `
                <div class="pill-row">
                  ${nonFunFactPills.map((pill) => `<span class="alert-pill ${pill.tone}">${pill.text}</span>`).join("")}
                </div>
              `
            : "";
          const funFactMarkup = person.funFact
            ? `<div class="alert-pill fun-fact"><strong>Fun fact:</strong> <span>${person.funFact}</span></div>`
            : "";
          return `
            <article class="roster-card ${expanded ? "is-expanded" : ""} ${riskLevelClass(person.churnRisk?.level, isNew)}" data-expand-card-id="${entry.personId}">
              <div class="roster-body">
                <header>
                  <div class="roster-identity">
                    <div class="roster-name">${person.name}</div>
                    <div class="meta-line">
                      <span>${selected.time}</span>
                      <span>•</span>
                      <span>${selected.title}</span>
                      <span>•</span>
                      <span>${person.membership}</span>
                    </div>
                  </div>
                  <div class="roster-top-right">
                    ${funFactMarkup}
                    ${
                      entry.bookingId
                        ? `
                          <div class="checkin-row">
                            <button
                              class="checkin-toggle ${checkIn?.checkedIn ? "is-checked" : ""}"
                              type="button"
                              data-checkin-id="${entry.bookingId}"
                              data-checkin-state="${checkIn?.checkedIn ? "checked-in" : "not-checked-in"}"
                              ${checkIn?.pending ? "disabled" : ""}
                            >
                              ${checkIn?.pending ? "Saving..." : checkIn?.checkedIn ? "Undo check-in" : "Check in"}
                            </button>
                            <span class="checkin-status ${checkIn?.tone ? `is-${checkIn.tone}` : ""}">
                              ${checkIn?.message || "Ready to check in."}
                            </span>
                          </div>
                        `
                        : ""
                    }
                    ${badgeMarkup}
                  </div>
                </header>
                ${
                  alertModel.primary
                    ? `<div class="alert-bar is-${alertModel.primary.tone}"><strong>${alertModel.primary.label}:</strong> ${alertModel.primary.text}</div>`
                    : ""
                }
                ${pillMarkup}
                <div class="metrics">
                  ${entry.stats
                    .map(
                      (stat) => `
                        <div class="metric">
                          <strong>${stat.value}</strong>
                          <span class="metric-label">${stat.label}</span>
                        </div>
                      `,
                    )
                    .join("")}
                </div>
                <div class="expand-row">
                  <span class="expand-summary">Concierge brief</span>
                  <button class="expand-button" type="button" data-expand-id="${entry.personId}">
                    <span>${expanded ? "▾" : "▸"}</span>
                    <span>${expanded ? "Hide Full Profile" : "See Full Profile"}</span>
                  </button>
                </div>
                <div class="expand-details">
                  <div class="expand-grid">
                    <div class="assumption-card">
                      <strong>Today matters</strong>
                      <p>${alertModel.primary ? `${alertModel.primary.label}: ${alertModel.primary.text}` : entry.visibleHighlights?.[0]?.value || "Warm, personal service will likely land best."}</p>
                    </div>
                    <div class="service-card celebration-spotlight">
                      <strong>${entry.expand.celebrationSpotlight?.title || "Celebration"}</strong>
                      <p>${entry.expand.celebrationSpotlight?.value || "No active celebration"}</p>
                      <small>${entry.expand.celebrationSpotlight?.note || "Use a warm, personal acknowledgment."}</small>
                    </div>
                    <div class="service-card membership-spotlight">
                      <strong>${entry.expand.membershipSpotlight?.title || "Membership fit"}</strong>
                      <p>${entry.expand.membershipSpotlight?.value || "Membership review"}</p>
                      <small>${entry.expand.membershipSpotlight?.note || "Review current plan and expiration."}</small>
                    </div>
                    <div class="service-card">
                      <strong>Team move</strong>
                      <p>${entry.expand.service}</p>
                    </div>
                    <div class="assumption-card">
                      <strong>Client read</strong>
                      <p>${entry.expand.assumption}</p>
                    </div>
                  </div>
                  <div class="breakdown-grid">
                    ${(entry.expand.breakdowns || [])
                      .map(
                        (section) => `
                          <article class="breakdown-card">
                            <strong>${section.title}</strong>
                            <div class="breakdown-list">
                              ${section.items.map((item) => `<span>${item}</span>`).join("")}
                            </div>
                          </article>
                        `,
                      )
                      .join("")}
                  </div>
                  <div class="inline-detail-grid">
                    ${person.profile.details
                      .filter(
                        (detail) =>
                          ![
                            "Fun fact",
                            "Preferred instructor",
                            "Preferred format",
                            "Membership",
                            "Churn risk",
                            "Lifetime classes",
                            "Last 30 days",
                            "Previous 30 days",
                          ].includes(detail.label),
                      )
                      .map(
                        (detail) => `
                          <div class="detail-item ${detailTone(detail.label)}">
                            <span>${detail.label}</span>
                            <strong>${detail.value}</strong>
                          </div>
                        `,
                      )
                      .join("")}
                  </div>
                  <div class="inline-notes">
                    ${(entry.expand.notes || []).map((note) => `<span class="chip">${note}</span>`).join("")}
                    ${person.profile.notes.map((note) => `<article><strong>Team prompt</strong><p>${note}</p></article>`).join("")}
                  </div>
                </div>
              </div>
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

function normalizeLiveData(payload) {
  if (!payload || typeof payload !== "object") return fallbackData;
  return {
    meta: payload.meta || {},
    summary: Array.isArray(payload.summary) ? payload.summary : fallbackData.summary,
    freshness: Array.isArray(payload.freshness) ? payload.freshness : fallbackData.freshness,
    celebrations: Array.isArray(payload.celebrations) ? payload.celebrations : fallbackData.celebrations,
    people: payload.people && typeof payload.people === "object" ? payload.people : fallbackData.people,
    frontdesk: Array.isArray(payload.frontdesk) ? payload.frontdesk : fallbackData.frontdesk,
    sessions: Array.isArray(payload.sessions) ? payload.sessions : fallbackData.sessions,
  };
}

function syncStateToCurrentData() {
  if (!currentData.sessions.some((session) => session.id === state.selectedSessionId)) {
    state.selectedSessionId = currentData.meta?.selectedSessionId || currentData.sessions[0]?.id || "";
  }
  const liveCheckInState = buildCheckInStateFromData(currentData);
  state.checkInByBookingId = Object.keys(liveCheckInState).length ? liveCheckInState : state.checkInByBookingId;
}

function render() {
  document.querySelectorAll(".toggle").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.view === state.view);
  });
  locationButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.location === state.location);
  });
  document.querySelectorAll(".board").forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.panel === state.view);
  });
  renderSummary();
  renderFrontdesk();
  renderSessions();
}

async function loadLiveDemoData() {
  try {
    const response = await fetch("/v1/demo/data", {
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) {
      throw new Error(`Live demo data failed with ${response.status}`);
    }
    const payload = await response.json();
    currentData = normalizeLiveData(payload);
    syncStateToCurrentData();
    render();
  } catch (error) {
    console.error("Failed to load live data", error);
    if (summaryStats) summaryStats.innerHTML = `<p style="grid-column:1/-1;text-align:center;color:#9F543F;padding:2rem;">Unable to load live data. Please refresh the page.</p>`;
  }
}

async function setCheckInState(bookingId, shouldCheckIn) {
  const current = state.checkInByBookingId[bookingId];
  if (!current || current.pending) return;

  state.checkInByBookingId[bookingId] = {
    ...current,
    pending: true,
    message: shouldCheckIn ? "Checking in with Momence..." : "Undoing check-in in Momence...",
    tone: "neutral",
  };
  render();

  try {
    const response = await fetch(`/v1/bookings/${bookingId}/check-in`, {
      method: shouldCheckIn ? "POST" : "DELETE",
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      let detail = "Momence rejected the update.";
      try {
        const errorPayload = await response.json();
        detail = errorPayload.detail || detail;
      } catch (error) {
        detail = response.statusText || detail;
      }
      throw new Error(detail);
    }

    state.checkInByBookingId[bookingId] = {
      checkedIn: shouldCheckIn,
      pending: false,
      message: shouldCheckIn ? "Checked in in Momence." : "Marked not checked in in Momence.",
      tone: "success",
    };
  } catch (error) {
    state.checkInByBookingId[bookingId] = {
      checkedIn: current.checkedIn,
      pending: false,
      message: error instanceof Error ? error.message : "Check-in update failed.",
      tone: "error",
    };
  }

  render();
}

document.querySelectorAll(".toggle").forEach((button) => {
  button.addEventListener("click", () => {
    state.view = button.dataset.view;
    render();
  });
});

locationButtons.forEach((button) => {
  button.addEventListener("click", () => {
    state.location = button.dataset.location;
    state.selectedSessionId = "";
    render();
  });
});

document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  const expandTrigger = target.closest("[data-expand-id]");
  if (expandTrigger) {
    event.preventDefault();
    event.stopPropagation();
    const personId = expandTrigger.dataset.expandId;
    if (state.expandedRosterIds.includes(personId)) {
      state.expandedRosterIds = state.expandedRosterIds.filter((item) => item !== personId);
    } else {
      state.expandedRosterIds = [...state.expandedRosterIds, personId];
    }
    renderSessions();
    return;
  }

  const expandCard = target.closest("[data-expand-card-id]");
  if (expandCard && !target.closest("[data-checkin-id]")) {
    const personId = expandCard.dataset.expandCardId;
    if (state.expandedRosterIds.includes(personId)) {
      state.expandedRosterIds = state.expandedRosterIds.filter((item) => item !== personId);
    } else {
      state.expandedRosterIds = [...state.expandedRosterIds, personId];
    }
    renderSessions();
    return;
  }

  const checkInTrigger = target.closest("[data-checkin-id]");
  if (checkInTrigger) {
    event.preventDefault();
    event.stopPropagation();
    const bookingId = checkInTrigger.dataset.checkinId;
    const checkIn = state.checkInByBookingId[bookingId];
    if (!bookingId || !checkIn) return;
    void setCheckInState(bookingId, !checkIn.checkedIn);
    return;
  }

  const sessionTrigger = target.closest("[data-session-id]");
  if (sessionTrigger) {
    state.selectedSessionId = sessionTrigger.dataset.sessionId;
    renderSessions();
  }
});

searchInput.addEventListener("input", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) return;
  state.query = target.value.trim().toLowerCase();
  render();
});

// Show loading state, then fetch live data — no fallback data shown
if (summaryStats) summaryStats.innerHTML = `<p style="grid-column:1/-1;text-align:center;color:#28200E;padding:2rem;">Loading live data...</p>`;
void loadLiveDemoData();
