function showHeadlines()
{
    $( '.item' ).each( function() {
        closeItem( $(this) );
    });
}

function showFullText() {
    $( '.item' ).each( function() {
        openItem( $(this) );
        hideItemSnippet( $(this) );
    });
}

function showSnippets() {
    $( '.item' ).each( function() {
        openItem( $(this) );
        showItemSnippet( $(this) );
    });
}

function onCloseItem() {
  var $item = $(this).closest( '.item' );
  closeItem( $item );
}

function onOpenItem() {
  // console.log( 'onOpenItem' );
  var $item = $(this).closest( '.item' );
  openItem( $item );
}

function showItemSnippet( $item )
{
  $item.find( '.item-summary,.item-content' ).hide();
  $item.find( '.item-snippet' ).show();
}

function hideItemSnippet( $item )
{
  $item.find( '.item-summary,.item-content' ).show();
  $item.find( '.item-snippet' ).hide();
}

function closeItem( $item ) {
  $item.find( '.item-body' ).hide();

  $item.find( '.item-close' ).hide();
  $item.find( '.item-open' ).show();
}

function openItem( $item ) {
  $item.find( '.item-body' ).show();

  $item.find( '.item-close' ).show();
  $item.find( '.item-open' ).hide();
}

$(document).ready( function() {
  $( '#show-headlines' ).click( showHeadlines );
  $( '#show-fulltext' ).click( showFullText );
  $( '#show-snippets' ).click( showSnippets );

  $( '.item-close,.item-collapse' ).click( onCloseItem );
  $( '.item-open,.item-expand' ).click( onOpenItem );
});
