from pybufrkit.dataquery import NodePath, PathComponent


def test_node_path_str():
    np = NodePath('')
    np.add_component(PathComponent('/', '001001', slice(None, None, None)))
    assert str(np) == '/001001[::]'

    np = NodePath('')
    np.subset_slice = 0
    np.add_component(PathComponent('/', '001001', 2))
    assert str(np) == '@[0]/001001[2]'

    np = NodePath('')
    np.subset_slice = slice(0, 7, 1)
    np.add_component(PathComponent('/', '001002', slice(None, None, None)))
    np.add_component(PathComponent('.', '031001', slice(0, None, None)))
    assert str(np) == '@[0:7:1]/001002[::].031001[0::]'
