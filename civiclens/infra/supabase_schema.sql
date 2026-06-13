-- CivicLens — Supabase PostgreSQL Schema
-- Run this in the Supabase SQL Editor to set up production tables.

-- ── Resources table ────────────────────────────────────────────────────────────
create table if not exists resources (
    id                  bigserial primary key,
    name                text not null,
    organization        text not null,
    category            text not null check (category in ('food','shelter','legal','health','utility','other')),
    subcategory         text,
    address             text not null,
    city                text not null,
    state               char(2) not null,
    zip                 text,
    phone               text,
    website             text,
    hours               text,
    languages_supported text default 'en',
    eligibility         text,
    accepts_walk_in     boolean default true,
    description         text,
    last_verified       date default current_date,
    created_at          timestamptz default now()
);

create index if not exists idx_resources_category on resources (category);
create index if not exists idx_resources_city     on resources (lower(city));
create index if not exists idx_resources_zip      on resources (zip);

-- Enable Row Level Security (public read, authenticated write)
alter table resources enable row level security;
create policy "Public read" on resources for select using (true);
create policy "Auth write"  on resources for insert with check (auth.role() = 'authenticated');
create policy "Auth update" on resources for update using (auth.role() = 'authenticated');

-- ── Responses table (Supabase Realtime delivery) ───────────────────────────────
create table if not exists responses (
    id               bigserial primary key,
    correlation_id   text not null unique,
    detected_language text not null,
    response_text    text not null,
    resources_count  int default 0,
    timestamp        timestamptz default now()
);

-- Automatically delete responses older than 24 hours (keep table lean)
create index if not exists idx_responses_correlation on responses (correlation_id);
create index if not exists idx_responses_timestamp   on responses (timestamp);

-- Enable Realtime on responses so frontend WebSocket receives updates instantly
alter publication supabase_realtime add table responses;

-- ── Seed data (import from CSV using Supabase Table Editor or COPY command) ───
-- COPY resources FROM '/path/to/resources.csv' CSV HEADER;
