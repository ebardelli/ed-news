document.addEventListener('DOMContentLoaded', () => {
    // Handle collapsible sections
    document.querySelectorAll('.collapsible').forEach(button => {
        button.addEventListener('click', () => {
            const content = button.nextElementSibling;
            const icon = button.querySelector('.icon');
            if (content.style.display === 'block') {
                content.style.display = 'none';
                icon.textContent = '+';
            } else {
                content.style.display = 'block';
                icon.textContent = '-';
            }
        });
    });

    // Handle modal functionality
    const modal = document.getElementById('abstract-modal');
    const modalContent = document.getElementById('modal-abstract');
    const closeModal = document.querySelector('.modal .close');

    document.querySelectorAll('.open-modal').forEach(link => {
        link.addEventListener('click', (event) => {
            event.preventDefault();
            modalContent.textContent = link.dataset.abstract;
            modal.style.display = 'block';
        });
    });

    closeModal.addEventListener('click', () => {
        modal.style.display = 'none';
    });

    window.addEventListener('click', (event) => {
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });

    // Toggle views based on ?view=articles or ?view=tocs (default 'articles')
    (function(){
        function getParam(name){
            const params = new URLSearchParams(window.location.search);
            return params.get(name);
        }
        const view = getParam('view') || 'articles';
        const articlesEl = document.getElementById('view-articles');
        const tocsEl = document.getElementById('view-tocs');
        const menuArticles = document.getElementById('menu-articles');
        const menuTocs = document.getElementById('menu-tocs');
        function show(v){
            if(v === 'articles'){
                if (articlesEl) articlesEl.style.display = '';
                if (tocsEl) tocsEl.style.display = 'none';
                if (menuArticles) menuArticles.classList.add('active');
                if (menuTocs) menuTocs.classList.remove('active');
            } else {
                if (articlesEl) articlesEl.style.display = 'none';
                if (tocsEl) tocsEl.style.display = '';
                if (menuArticles) menuArticles.classList.remove('active');
                if (menuTocs) menuTocs.classList.add('active');
            }
        }
        if(articlesEl || tocsEl){
            show(view);
        }
    })();
});