const resources = {
  en: {
    translation: {
      nav: {
        why: "Why Ditto",
        capabilities: "Capabilities",
        interfaces: "Interfaces",
        ops: "Ops"
      },
      hero: {
        eyebrow: "Distributed cache, without the drama",
        title: "Fast cache. Clear behavior.",
        lead: "Ditto is a strongly consistent distributed in-memory key-value cache built to stay predictable under real usage. The fast path matters, but so do the daily touchpoints: APIs, watch flows, namespaces, admin UI, CLI, and observability.",
        primaryCta: "Show the capabilities",
        secondaryCta: "See how it is used",
        point1: "Local reads, strong write consistency",
        point2: "HTTP and TCP access to the same cache core",
        point3: "TTL, LFU eviction, watch, pattern ops, namespaces",
        signal1: "Fast GETs without unnecessary detours.",
        signal2: "A write is done when the system says it is really done.",
        signal3: "Binary TCP and HTTP API on the same core behavior.",
        signal4: "Dashboard, CLI, stats, health summary, admin control."
      },
      why: {
        eyebrow: "Why it matters",
        title: "Not just a cache engine, but a cache system that feels usable.",
        lead: "Ditto is not built around \"it will probably work somehow\". It is built around making the cache layer predictable for applications and manageable for operators. The result is something fast, visible, and developer-friendly at the same time.",
        card1Title: "Predictable reads and writes",
        card1Text: "Local reads stay fast, while the write model stays strongly consistent. That is useful when you do not want every cache decision to come with a paragraph of caveats.",
        card2Title: "Comfortable from the app side",
        card2Text: "Alongside basic GET, SET and DELETE, you get batch writes, watch, TTL updates by pattern, deletes by pattern, and namespace support. Less helper code around the cache, more actual product work.",
        card3Title: "Clear from the ops side",
        card3Text: "Node state, health summary, memory usage, evictions, request and error stats, admin UI and CLI together make the system feel manageable instead of mysterious."
      },
      capabilities: {
        eyebrow: "Capabilities",
        title: "The things that make Ditto easy to move with.",
        item1Title: "TTL and memory limits",
        item1Text: "Per-key TTL, global default TTL, background sweep and LFU eviction. The cache does not just grow forever, it behaves like a system that expects real load.",
        item2Title: "Watch flows over TCP",
        item2Text: "If you want to follow key changes in near real time, watch and unwatch plus pushed watch events are already part of the model.",
        item3Title: "Pattern-based operations",
        item3Text: "Delete keys by pattern, or update TTL for a whole key group. Very handy for sessions, feature caches, or tenant-scoped keyspaces.",
        item4Title: "Namespaces and quota",
        item4Text: "Namespaces separate the cache logically, while quota helps make sure one tenant or use case cannot crowd everyone else out.",
        item5Title: "Persistence when you need it",
        item5Text: "Backup, export and import paths are part of the story too, so Ditto is not limited to purely in-memory thinking. Persistence can be controlled explicitly instead of being an accidental side effect.",
        item6Title: "HTTP and TCP together",
        item6Text: "REST API for straightforward integrations, binary TCP when you want a leaner or more interactive model. You do not have to force every client into one style.",
        item7Title: "Healthy diagnostics",
        item7Text: "Stats, health summary, latency estimates, error breakdowns, quota pressure, hot-key and repair signals. Not a black box, but an observable component."
      },
      interfaces: {
        eyebrow: "Interfaces",
        title: "You do not need a long explanation to start using it.",
        lead: "On the data side, Ditto gives you two clear entry points: HTTP and TCP. That means teams can use it in the way that feels natural for them.",
        httpTitle: "HTTP API",
        httpText: "A good fit when you want quick integrations, scripts, or simple service-to-service communication over REST.",
        tcpTitle: "TCP protocol",
        tcpText: "A good fit when your application wants a persistent connection, watch events, or lower protocol overhead."
      },
      ops: {
        eyebrow: "For operators",
        title: "The admin side is part of the product, not an afterthought.",
        lead: "`ditto-mgmt` gives you a web UI and admin API, while `dittoctl` gives you a scriptable CLI. So the same cache can stay fast and still remain manageable when multiple nodes, namespaces and use cases are involved.",
        card1Title: "Dashboard and node control",
        card1Text: "Real-time cluster view, node states, memory usage, active and syncing states, and the typical controls operators actually need.",
        card2Title: "Health and metrics mindset",
        card2Text: "Health summary and stats do not just answer \"is it alive\", but also show how the cache behaves under pressure and around failure paths.",
        card3Title: "Security and control",
        card3Text: "Auth, TLS, policy gates and admin channels push Ditto toward deliberate, controlled usage instead of accidental exposure. The same goes for persistence: backup and restore paths are there, but they stay explicitly governed."
      },
      clients: {
        eyebrow: "Clients",
        title: "It is not tied to one language.",
        lead: "The related Ditto clients live in a separate repository, and there is support for Go, Java, Node.js and Python. For this site, the important part is simply that Ditto can fit multi-language teams without drama.",
        pill1: "Go client",
        pill2: "Java client",
        pill3: "Node.js client",
        pill4: "Python client",
        pill5: "HTTP + TCP",
        pill6: "Watch support"
      },
      footer: {
        brand: "Ditto Cache",
        copy: "Fast, consistent distributed cache with practical APIs, observability, and admin control.",
        link1: "Why Ditto",
        link2: "Capabilities",
        link3: "Interfaces",
        link4: "Ops"
      }
    }
  },
  hu: {
    translation: {
      nav: {
        why: "Mi ez",
        capabilities: "Kepessegek",
        interfaces: "Hasznalat",
        ops: "Operatoroknak"
      },
      hero: {
        eyebrow: "Distributed cache, sallang nelkul",
        title: "Gyors cache. Tiszta mukodes.",
        lead: "A Ditto egy strongly-consistent distributed in-memory key-value cache. Arra keszult, hogy valodi hasznalat mellett is kiszamithato maradjon. A gyorsasag fontos, de ugyanilyen fontosak a napi erintkezesi pontok is: API-k, watch flow-k, namespace-ek, admin UI, CLI es megfigyelhetoseg.",
        primaryCta: "Mutasd a kepessegeket",
        secondaryCta: "Nezzuk a hasznalatot",
        point1: "Helyi olvasas, eros irasi konzisztencia",
        point2: "HTTP es TCP eleres ugyanarra a cache magra",
        point3: "TTL, LFU eviction, watch, pattern muveletek, namespace-ek",
        signal1: "Gyors GET-ek folosleges kerulok nelkul.",
        signal2: "Az iras akkor kesz, amikor a rendszer szerint tenyleg kesz.",
        signal3: "Binaris TCP es HTTP API ugyanazzal a viselkedessel.",
        signal4: "Dashboard, CLI, stats, health summary es admin kontroll."
      },
      why: {
        eyebrow: "Miert erdekes",
        title: "Nem csak egy cache engine, hanem egy jol hasznalhato cache rendszer.",
        lead: "A Ditto nem arra epit, hogy \"valahogy majd mukodik\". Arra epit, hogy a cache reteg kiszamithato legyen alkalmazasoldalon, es kezelheto maradjon operatori oldalrol is. Az eredmeny valami gyors, atlathato es fejlesztobarat egyszerre.",
        card1Title: "Kiszamithato olvasas es iras",
        card1Text: "A helyi olvasas gyors marad, mikozben az irasi modell eros konzisztenciara epit. Ez jo, amikor nem akarsz minden cache dontes melle hosszu megjegyzeseket irni.",
        card2Title: "Kenyelmes alkalmazasoldalrol",
        card2Text: "Az alap GET, SET es DELETE mellett van batch iras, watch, TTL pattern alapjan, delete pattern alapjan es namespace tamogatas is. Kevesebb segedkod, tobb valodi termekmunka.",
        card3Title: "Atlathato operatori oldalrol",
        card3Text: "Node allapot, health summary, memoriahasznalat, eviction, request- es hibastatisztikak, admin UI es CLI egyutt adjak azt az erzest, hogy a rendszer tenyleg kezelheto."
      },
      capabilities: {
        eyebrow: "Kepessegek",
        title: "Azok a dolgok, amik miatt a Ditto-val konnyu haladni.",
        item1Title: "TTL es memoriahatar",
        item1Text: "Per-key TTL, globalis default TTL, hatterben futo sweep es LFU eviction. A cache nem csak no, hanem ugy viselkedik, mint egy valodi terhelesre felkeszitett rendszer.",
        item2Title: "Watch flow-k TCP-n",
        item2Text: "Ha valos ideju kulcsvalltozasokat akarsz kovetni, a watch, unwatch es a pusholt watch eventek mar eleve reszei a modellnek.",
        item3Title: "Pattern alapu muveletek",
        item3Text: "Lehet kulcsokat torolni mintara, vagy TTL-t allitani egy teljes kulcscsoportra. Ez nagyon jol jon sessionoknel, feature cache-eknel vagy tenant scoped kulcstereknel.",
        item4Title: "Namespace-ek es quota",
        item4Text: "A namespace-ekkel a cache logikailag szeparalhato, a quota pedig segit abban, hogy egy tenant vagy use case ne szorithassa ki a tobbieket.",
        item5Title: "Perzisztalas, ha kell",
        item5Text: "A backup, export es import lehetosegek is reszei a kepnek, vagyis a Ditto nem csak tisztan in-memory gondolkodasra jo. A perzisztalas kontrollalt kepesseg, nem veletlen mellekhatas.",
        item6Title: "HTTP es TCP egyszerre",
        item6Text: "Van REST API a gyors integraciokhoz, es van binaris TCP protokoll, ha hatekonyabb vagy interaktivabb mukodes kell. Nem kell minden klienst ugyanabba a stilusba kenyszeriteni.",
        item7Title: "Normalis diagnosztika",
        item7Text: "Stats, health summary, latency becslesek, error bontasok, quota nyomas, hot-key es repair jelzesek. Nem fekete doboz, hanem megfigyelheto komponens."
      },
      interfaces: {
        eyebrow: "Hasznalat",
        title: "Nem kell hosszu magyarazat, hogy el lehessen kezdeni hasznalni.",
        lead: "Adatoldalon a Ditto ket jol ertheto belepesi pontot ad: HTTP-t es TCP-t. Igy a csapat ott tudja hasznalni, ahol ez termeszetesnek erzodik.",
        httpTitle: "HTTP API",
        httpText: "Jo valasztas, ha gyors integraciot, scriptelest vagy egyszeru service-to-service REST kommunikaciot akarsz.",
        tcpTitle: "TCP protokoll",
        tcpText: "Jo valasztas, ha az alkalmazasodnak tartos kapcsolat, watch esemenyek vagy kisebb protocol overhead kell."
      },
      ops: {
        eyebrow: "Operatoroknak",
        title: "Az admin oldal is resze a termeknek, nem utolag felrakott extra.",
        lead: "A `ditto-mgmt` webes feluletet es admin API-t ad, a `dittoctl` pedig scriptelheto CLI-t. Igy ugyanaz a cache gyors maradhat, es kozben kezelheto is, amikor tobb node, namespace vagy use case fut rajta.",
        card1Title: "Dashboard es node kontroll",
        card1Text: "Valos ideju cluster kep, node allapotok, memoriahasznalat, active es syncing statuszok, plusz azok a kontrollok, amikre operatori oldalon tenyleg szukseg van.",
        card2Title: "Health es metrics szemlelet",
        card2Text: "A health summary es a statisztikak nem csak azt mondjak meg, hogy \"el-e\", hanem azt is, hogyan viselkedik a cache terheles es hibak kozben.",
        card3Title: "Biztonsag es kontroll",
        card3Text: "Auth, TLS, policy gate-ek es admin csatornak mellett a Ditto inkabb a kontrollalt hasznalat fele huz, nem a veletlenul nyitva hagyott mukodes fele. Ugyanez igaz a perzisztalasra is: a backup es restore utak megvannak, de tudatosan szabalyozottak."
      },
      clients: {
        eyebrow: "Kliensek",
        title: "Nem egyetlen nyelvre van kitalalva.",
        lead: "A kapcsolodo Ditto kliensek kulon repositoryban elnek, es jelenleg Go, Java, Node.js es Python iranyban is van tamogatas. A lenyeg itt most csak annyi, hogy a Ditto jol illeszkedik tobbnyelvu csapatokhoz is.",
        pill1: "Go client",
        pill2: "Java client",
        pill3: "Node.js client",
        pill4: "Python client",
        pill5: "HTTP + TCP",
        pill6: "Watch support"
      },
      footer: {
        brand: "Ditto Cache",
        copy: "Gyors, konzisztens distributed cache jol hasznalhato API-kkal, megfigyelhetoseggel es admin kontrollal.",
        link1: "Mi ez",
        link2: "Kepessegek",
        link3: "Hasznalat",
        link4: "Operatoroknak"
      }
    }
  }
};

function applyTranslations() {
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.getAttribute("data-i18n");
    const value = i18next.t(key);
    if (typeof value === "string") {
      node.textContent = value;
    }
  });

  const current = i18next.language || "en";
  document.documentElement.lang = current;
  const toggleLabel = document.getElementById("langToggleLabel");
  if (toggleLabel) {
    toggleLabel.textContent = current === "en" ? "HU" : "EN";
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  await i18next.init({
    lng: "en",
    fallbackLng: "en",
    resources
  });

  applyTranslations();

  const langToggle = document.getElementById("langToggle");
  if (langToggle) {
    langToggle.addEventListener("click", async () => {
      const nextLang = i18next.language === "en" ? "hu" : "en";
      await i18next.changeLanguage(nextLang);
      applyTranslations();
    });
  }
});
