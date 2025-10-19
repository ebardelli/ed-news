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
});