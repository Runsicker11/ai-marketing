<?php
/**
 * Plugin Name: PE — Yoast SEO Meta via REST API
 * Description: Registers Yoast SEO title and meta description fields as
 *              writable via the WordPress REST API. Required for the
 *              Pickleball Effect AI marketing system to push SEO meta updates.
 * Version: 1.0
 */

add_action( 'init', function () {
    $post_types = [ 'post', 'page' ];
    $fields = [
        '_yoast_wpseo_title'    => 'Yoast SEO title',
        '_yoast_wpseo_metadesc' => 'Yoast SEO meta description',
    ];

    foreach ( $post_types as $post_type ) {
        foreach ( $fields as $key => $description ) {
            register_meta( 'post', $key, [
                'object_subtype' => $post_type,
                'type'           => 'string',
                'single'         => true,
                'description'    => $description,
                'show_in_rest'   => true,
                'auth_callback'  => function () {
                    return current_user_can( 'edit_posts' );
                },
            ] );
        }
    }
} );
