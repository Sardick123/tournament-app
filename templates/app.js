// frontend/app.js
document.addEventListener('DOMContentLoaded', () => {
    const tg = window.Telegram.WebApp;
    tg.ready();
    tg.expand(); // Expand the app to full height

    // --- Configuration ---
    // IMPORTANT: Replace this with your public API URL from Render
    const API_URL = 'https://your-api-name.onrender.com';

    // --- State ---
    let currentUser = tg.initDataUnsafe?.user;
    let currentTournamentId = null;

    // --- DOM Elements ---
    const views = {
        list: document.getElementById('view-list'),
        details: document.getElementById('view-details'),
    };
    const tournamentListEl = document.getElementById('tournament-list');
    const detailsTitleEl = document.getElementById('details-title');
    const detailsContentEl = document.getElementById('details-content');
    const detailsPlayersEl = document.getElementById('details-players-list');
    const joinButton = document.getElementById('join-button');

    // --- View Management ---
    function showView(viewId) {
        Object.values(views).forEach(view => view.classList.remove('active'));
        views[viewId].classList.add('active');

        if (viewId === 'list') {
            tg.BackButton.hide();
        } else {
            tg.BackButton.show();
        }
    }

    tg.BackButton.onClick(() => {
        showView('list');
        loadTournaments();
    });

    // --- Data Fetching & Rendering ---
    async function loadTournaments() {
        tournamentListEl.innerHTML = '<div class="loader"></div>';
        try {
            const response = await fetch(`${API_URL}/api/tournaments`);
            const tournaments = await response.json();
            renderTournamentList(tournaments);
        } catch (error) {
            tournamentListEl.innerText = 'Failed to load tournaments.';
            console.error(error);
        }
    }

    function renderTournamentList(tournaments) {
        tournamentListEl.innerHTML = '';
        if (tournaments.length === 0) {
            tournamentListEl.innerText = 'No tournaments yet!';
            return;
        }
        tournaments.forEach(t => {
            const card = document.createElement('div');
            card.className = 'tournament-card';
            card.innerHTML = `
                <h2>${t.name}</h2>
                <p>${t.game}</p>
                <p class="status ${t.status}">${t.status.charAt(0).toUpperCase() + t.status.slice(1)}</p>
            `;
            card.onclick = () => loadTournamentDetails(t.id);
            tournamentListEl.appendChild(card);
        });
    }

    async function loadTournamentDetails(tournamentId) {
        currentTournamentId = tournamentId;
        showView('details');
        detailsContentEl.innerHTML = '<div class="loader"></div>';
        detailsPlayersEl.innerHTML = '';
        joinButton.style.display = 'none';

        try {
            const response = await fetch(`${API_URL}/api/tournaments/${tournamentId}`);
            const details = await response.json();

            detailsTitleEl.innerText = details.name;
            detailsContentEl.innerHTML = `<p><strong>Game:</strong> ${details.game}</p><p><strong>Status:</strong> ${details.status}</p>`;

            detailsPlayersEl.innerHTML = '<h3>Players</h3>';
            if (details.players.length > 0) {
                details.players.forEach(p => {
                    const playerEl = document.createElement('div');
                    playerEl.className = 'player-item';
                    playerEl.innerText = p.username;
                    detailsPlayersEl.appendChild(playerEl);
                });
            } else {
                detailsPlayersEl.innerHTML += '<p>No players have registered yet.</p>';
            }

            // Show join button if tournament is pending and user hasn't joined
            const userHasJoined = details.players.some(p => p.user_id === currentUser.id);
            if (details.status === 'pending' && !userHasJoined) {
                joinButton.style.display = 'block';
                joinButton.disabled = false;
                joinButton.innerText = 'Join Tournament';
            }

        } catch (error) {
            detailsContentEl.innerText = 'Failed to load details.';
            console.error(error);
        }
    }

    // --- User Actions ---
    joinButton.onclick = async () => {
        if (!currentUser || !currentTournamentId) return;

        joinButton.disabled = true;
        joinButton.innerText = 'Joining...';

        try {
            const response = await fetch(`${API_URL}/api/tournaments/${currentTournamentId}/join`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    user_id: currentUser.id,
                    username: currentUser.username || `${currentUser.first_name} ${currentUser.last_name || ''}`.trim()
                })
            });

            if (response.ok) {
                tg.showAlert('You have successfully joined the tournament!');
                loadTournamentDetails(currentTournamentId); // Refresh details
            } else {
                const errorData = await response.json();
                tg.showAlert(`Could not join: ${errorData.message}`);
                joinButton.disabled = false;
                joinButton.innerText = 'Join Tournament';
            }
        } catch (error) {
            tg.showAlert('An error occurred. Please try again.');
            console.error(error);
            joinButton.disabled = false;
            joinButton.innerText = 'Join Tournament';
        }
    };


    // --- Initial Load ---
    loadTournaments();
});
