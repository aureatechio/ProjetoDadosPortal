-- ==============================================================
-- Script para configurar políticas RLS no bucket "portal" do Supabase Storage
-- Execute este SQL no SQL Editor do Supabase Dashboard:
-- https://supabase.com/dashboard/project/[SEU_PROJECT]/sql/new
-- ==============================================================

-- 1. Verifica se o bucket "portal" existe, senão cria
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
    'portal',
    'portal',
    true,  -- bucket público (imagens podem ser acessadas por URL)
    5242880,  -- 5MB limite por arquivo
    ARRAY['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/svg+xml']
)
ON CONFLICT (id) DO UPDATE SET
    public = true,
    file_size_limit = 5242880,
    allowed_mime_types = ARRAY['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/svg+xml'];

-- 2. Remove políticas antigas (se existirem) para evitar conflitos
DROP POLICY IF EXISTS "Permitir leitura pública" ON storage.objects;
DROP POLICY IF EXISTS "Permitir upload público" ON storage.objects;
DROP POLICY IF EXISTS "Permitir update público" ON storage.objects;
DROP POLICY IF EXISTS "Permitir delete público" ON storage.objects;
DROP POLICY IF EXISTS "portal_public_read" ON storage.objects;
DROP POLICY IF EXISTS "portal_public_insert" ON storage.objects;
DROP POLICY IF EXISTS "portal_public_update" ON storage.objects;
DROP POLICY IF EXISTS "portal_public_delete" ON storage.objects;

-- 3. Política de LEITURA pública (qualquer um pode ver as imagens)
CREATE POLICY "portal_public_read"
ON storage.objects FOR SELECT
TO public
USING (bucket_id = 'portal');

-- 4. Política de INSERT (permite upload de imagens)
-- Permite que qualquer requisição (inclusive com chave anon) faça upload
CREATE POLICY "portal_public_insert"
ON storage.objects FOR INSERT
TO public
WITH CHECK (bucket_id = 'portal');

-- 5. Política de UPDATE (permite sobrescrever imagens - upsert)
CREATE POLICY "portal_public_update"
ON storage.objects FOR UPDATE
TO public
USING (bucket_id = 'portal')
WITH CHECK (bucket_id = 'portal');

-- 6. Política de DELETE (permite remover imagens antigas)
CREATE POLICY "portal_public_delete"
ON storage.objects FOR DELETE
TO public
USING (bucket_id = 'portal');

-- ==============================================================
-- Verificação: liste as políticas criadas
-- ==============================================================
SELECT 
    policyname,
    tablename,
    permissive,
    roles,
    cmd,
    qual,
    with_check
FROM pg_policies 
WHERE tablename = 'objects' 
AND schemaname = 'storage'
AND policyname LIKE 'portal%';
