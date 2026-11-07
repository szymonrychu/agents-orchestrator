-- spec.kube_access became spec.kube_cluster_access (T-55); no alias. Idempotent.
UPDATE sessions SET spec = (spec - 'kube_access')
    || jsonb_build_object('kube_cluster_access', spec -> 'kube_access')
WHERE spec ? 'kube_access';
UPDATE presets SET spec = (spec - 'kube_access')
    || jsonb_build_object('kube_cluster_access', spec -> 'kube_access')
WHERE spec ? 'kube_access';
